package models

import (
	"errors"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDiggerJobTransitionConflict = errors.New("stale, conflicting or unattributed job callback")
var ErrDiggerJobCallbackForbidden = errors.New("job callback outside organisation repository scope")

// AcceptDiggerJobCallback validates the current attempt and persists its status
// under the same batch/job lock order as dependency retirement. No handler save
// may later overwrite a different accepted callback revision.
func (db *Database) AcceptDiggerJobCallback(reported *DiggerJob, orgID uint) (*DiggerJob, error) {
	var saved DiggerJob
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		var batch DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", reported.BatchID).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&saved, "id = ? AND digger_job_id = ? AND batch_id = ?", reported.ID, reported.DiggerJobID, reported.BatchID).Error; err != nil {
			return err
		}
		if saved.DependencyRetiredAt != nil {
			return ErrDiggerJobRetired
		}
		var owned int64
		if err := tx.Model(&Repo{}).Where("organisation_id = ? AND repo_full_name = ?", orgID, batch.RepoFullName).Count(&owned).Error; err != nil {
			return err
		}
		if owned != 1 {
			return ErrDiggerJobCallbackForbidden
		}
		if batch.VCS == DiggerVCSGithub {
			var linked int64
			if err := tx.Model(&GithubAppInstallationLink{}).Where("organisation_id = ? AND github_installation_id = ? AND status = ?", orgID, batch.GithubInstallationId, GithubAppInstallationLinkActive).Count(&linked).Error; err != nil {
				return err
			}
			if linked == 0 {
				return ErrDiggerJobCallbackForbidden
			}
		}
		// Qualification belongs to persisted jobs, not this replica's current
		// producer flag. Include retained deleted rows so removal cannot weaken it.
		var qualified int64
		if err := tx.Unscoped().Model(&DiggerJob{}).Where("batch_id = ? AND dispatch_protocol_version <> 0", batch.ID.String()).Count(&qualified).Error; err != nil {
			return err
		}
		if !callbackAdvancesJob(saved, *reported, qualified > 0) {
			return ErrDiggerJobTransitionConflict
		}
		now := time.Now().UTC()
		next := saved
		next.Status, next.StatusUpdatedAt = reported.Status, reported.StatusUpdatedAt
		next.WorkflowRunID, next.WorkflowRunAttempt = reported.WorkflowRunID, reported.WorkflowRunAttempt
		if reported.WorkflowRunUrl != nil {
			next.WorkflowRunUrl = reported.WorkflowRunUrl
		}
		if reported.Status == scheduler.DiggerJobSucceeded || reported.Status == scheduler.DiggerJobFailed {
			next.TerraformOutput = reported.TerraformOutput
		}
		if reported.Status == scheduler.DiggerJobSucceeded {
			next.PlanFootprint, next.PRCommentUrl, next.PRCommentId = reported.PlanFootprint, reported.PRCommentUrl, reported.PRCommentId
		}
		result := tx.Exec(`UPDATE digger_jobs SET status = ?, status_updated_at = ?, workflow_run_id = ?, workflow_run_attempt = ?, workflow_run_url = ?,
            terraform_output = ?, plan_footprint = ?, pr_comment_url = ?, pr_comment_id = ?,
            callback_revision = callback_revision + 1, workflow_dispatch_started_at = COALESCE(workflow_dispatch_started_at, ?), updated_at = ?
            WHERE id = ? AND callback_revision = ? AND dependency_retired_at IS NULL AND deleted_at IS NULL`,
			next.Status, next.StatusUpdatedAt, next.WorkflowRunID, next.WorkflowRunAttempt, next.WorkflowRunUrl, next.TerraformOutput, next.PlanFootprint, next.PRCommentUrl, next.PRCommentId, now, now, saved.ID, saved.CallbackRevision)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDiggerJobTransitionConflict
		}
		id := saved.ID
		saved = DiggerJob{}
		if err := tx.Preload("DiggerJobSummary").First(&saved, id).Error; err != nil {
			return err
		}
		saved.Batch = &batch
		return nil
	})
	return &saved, err
}

func callbackAdvancesJob(current, next DiggerJob, qualifiedBatch bool) bool {
	if next.WorkflowRunID < 0 || next.WorkflowRunAttempt < 0 || ((next.WorkflowRunID == 0) != (next.WorkflowRunAttempt == 0)) {
		return false
	}
	terminal := next.Status == scheduler.DiggerJobFailed || next.Status == scheduler.DiggerJobSucceeded
	if qualifiedBatch && current.WorkflowRunID == 0 && current.Status == scheduler.DiggerJobStarted && terminal {
		return false
	}
	// Unknown lineage can complete only an unqualified batch. A legacy report
	// never clears known lineage or authorizes qualified irreversible effects.
	if next.WorkflowRunID == 0 || next.WorkflowRunAttempt == 0 {
		if current.WorkflowRunID != 0 || current.WorkflowRunAttempt != 0 || (qualifiedBatch && terminal) {
			return false
		}
	} else if current.WorkflowRunID != 0 {
		if next.WorkflowRunID != current.WorkflowRunID || next.WorkflowRunAttempt < current.WorkflowRunAttempt {
			return false
		}
		if next.WorkflowRunAttempt > current.WorkflowRunAttempt {
			return next.Status == scheduler.DiggerJobStarted
		}
	}
	hasCallbackTime := current.CallbackRevision > 0 || current.WorkflowRunID > 0 ||
		current.Status == scheduler.DiggerJobStarted || current.Status == scheduler.DiggerJobFailed || current.Status == scheduler.DiggerJobSucceeded
	if hasCallbackTime && !current.StatusUpdatedAt.IsZero() && (next.StatusUpdatedAt.IsZero() || next.StatusUpdatedAt.Before(current.StatusUpdatedAt)) {
		return false
	}
	rank := func(status scheduler.DiggerJobStatus) int {
		switch status {
		case scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun:
			return 1
		case scheduler.DiggerJobTriggered:
			return 2
		case scheduler.DiggerJobStarted:
			return 3
		case scheduler.DiggerJobSucceeded, scheduler.DiggerJobFailed:
			return 4
		}
		return 0
	}
	if rank(next.Status) < rank(current.Status) {
		return false
	}
	if rank(current.Status) == 4 && next.Status != current.Status {
		return false
	}
	if hasCallbackTime && !current.StatusUpdatedAt.IsZero() && next.StatusUpdatedAt.Equal(current.StatusUpdatedAt) && current.Status != next.Status {
		return false
	}
	return true
}
