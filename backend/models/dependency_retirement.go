package models

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDiggerJobRetired = errors.New("job permanently retired after failed prerequisite")

// ObserveDiggerJobCallback competes with retirement before acknowledging a runner.
// Even a created callback without run metadata is evidence of possible execution.
func (db *Database) ObserveDiggerJobCallback(job *DiggerJob) (bool, error) {
	result := db.GormDB.Exec(`UPDATE digger_jobs
        SET workflow_dispatch_started_at = COALESCE(workflow_dispatch_started_at, ?)
        WHERE id = ? AND digger_job_id = ? AND batch_id = ?
        AND deleted_at IS NULL AND dependency_retired_at IS NULL`,
		time.Now().UTC(), job.ID, job.DiggerJobID, job.BatchID)
	return result.RowsAffected == 1, result.Error
}

type DependencyReconciliation struct {
	RetiredJobs       []string                    `json:"retired_jobs"`
	UnresolvedJobs    []string                    `json:"unresolved_jobs"`
	BatchStatus       scheduler.DiggerBatchStatus `json:"batch_status"`
	UnresolvedReasons map[string]string           `json:"unresolved_reasons"`
}

// ReconcileFailedDependencies uses stored edges, never current PR configuration.
// Row locks and the dispatch CAS choose one winner. Claims are never reclaimed.
func (db *Database) ReconcileFailedDependencies(batchID uuid.UUID) (*DependencyReconciliation, error) {
	return db.reconcileFailedDependencies(batchID, nil)
}

// ReconcileCommandDependencies binds operator recovery to an original sealed command.
func (db *Database) ReconcileCommandDependencies(orgID uint, repo string, pr int, comment int64, head string, batchID uuid.UUID) (*DependencyReconciliation, error) {
	return db.reconcileFailedDependencies(batchID, func(tx *gorm.DB, batch DiggerBatch) error {
		evidence, err := (&Database{GormDB: tx}).GetCommandProvenance(orgID, repo, pr, comment)
		if err != nil {
			return err
		}
		for _, entry := range evidence.Batches {
			if entry.BatchID == batchID.String() && entry.ManifestComplete && entry.Origin.CommitSHA == head && batch.CommitSha == head {
				return nil
			}
		}
		return errors.New("recovery requires matching repository, source, head and complete sealed manifest")
	})
}

func (db *Database) reconcileFailedDependencies(batchID uuid.UUID, verify func(*gorm.DB, DiggerBatch) error) (*DependencyReconciliation, error) {
	result := &DependencyReconciliation{RetiredJobs: []string{}, UnresolvedJobs: []string{}, UnresolvedReasons: map[string]string{}}
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		var batch DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", batchID).Error; err != nil {
			return err
		}
		var jobs []DiggerJob
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("batch_id = ?", batchID.String()).Order("id ASC").Limit(MaxCommandProvenanceJobs + 1).Find(&jobs).Error; err != nil {
			return err
		}
		if len(jobs) > MaxCommandProvenanceJobs {
			return ErrProvenanceTooLarge
		}
		byID := map[string]*DiggerJob{}
		ids := make([]string, 0, len(jobs))
		for i := range jobs {
			job := &jobs[i]
			if job.DeletedAt.Valid || job.DiggerJobID == "" || byID[job.DiggerJobID] != nil {
				return errors.New("dependency graph has missing or duplicate job identities")
			}
			byID[job.DiggerJobID] = job
			ids = append(ids, job.DiggerJobID)
		}
		var links []DiggerJobParentLink
		if len(ids) > 0 {
			if err := tx.Where("digger_job_id IN ? OR parent_digger_job_id IN ?", ids, ids).Find(&links).Error; err != nil {
				return err
			}
		}
		parents := map[string][]string{}
		for _, link := range links {
			if byID[link.DiggerJobId] == nil || byID[link.ParentDiggerJobId] == nil {
				return errors.New("dependency graph crosses batch or has missing job")
			}
			parents[link.DiggerJobId] = append(parents[link.DiggerJobId], link.ParentDiggerJobId)
		}
		// Validate the whole graph before persisting any fence.
		visited, visiting := map[string]bool{}, map[string]bool{}
		var visit func(string) error
		visit = func(id string) error {
			if visiting[id] {
				return errors.New("dependency graph contains a cycle")
			}
			if visited[id] {
				return nil
			}
			visiting[id] = true
			for _, parent := range parents[id] {
				if err := visit(parent); err != nil {
					return err
				}
			}
			visiting[id], visited[id] = false, true
			return nil
		}
		for _, id := range ids {
			if err := visit(id); err != nil {
				return err
			}
		}
		if verify != nil {
			if err := verify(tx, batch); err != nil {
				return err
			}
		}
		// Propagate failure through running intermediates without retiring them.
		failures := map[string]string{}
		var failedPrerequisite func(string) string
		failedPrerequisite = func(id string) string {
			if reason, exists := failures[id]; exists {
				return reason
			}
			for _, parentID := range parents[id] {
				parent := byID[parentID]
				if parent.DependencyRetiredAt != nil || (parent.Status == scheduler.DiggerJobFailed && parent.WorkflowRunID > 0 && parent.WorkflowRunAttempt > 0) {
					failures[id] = parentID
					return parentID
				}
				if parent.Status == scheduler.DiggerJobFailed {
					result.UnresolvedReasons[id] = "prerequisite_execution_unknown"
				}
				if ancestor := failedPrerequisite(parentID); ancestor != "" {
					failures[id] = ancestor
					return ancestor
				}
			}
			failures[id] = ""
			return ""
		}
		// Recursive failure lookup covers every descendant in this transaction.
		for i := range jobs {
			job := &jobs[i]
			if job.DependencyRetiredAt != nil || (job.Status != scheduler.DiggerJobCreated && job.Status != scheduler.DiggerJobQueuedForRun) {
				continue
			}
			parentID := failedPrerequisite(job.DiggerJobID)
			if parentID != "" {
				// The creation marker is positive evidence that every send
				// consumed the durable claim. Never infer it for old rows.
				if job.DispatchProtocolVersion != 1 || jobHasExecutionEvidence(*job) {
					continue
				}
				executions, err := jobHasWorkflowLink(tx, job.DiggerJobID)
				if err != nil {
					return err
				}
				if executions {
					result.UnresolvedReasons[job.DiggerJobID] = "execution_active_or_uncertain"
					continue
				}
				now := time.Now().UTC()
				update := tx.Exec(`UPDATE digger_jobs SET dependency_retired_at = ?, dependency_failure_job_id = ?, status = ?, status_updated_at = ?, updated_at = ?
                        WHERE id = ? AND batch_id = ? AND digger_job_id = ? AND deleted_at IS NULL
                        AND dispatch_protocol_version = 1 AND dependency_retired_at IS NULL
                        AND workflow_dispatch_started_at IS NULL AND status IN (?, ?)`,
					now, parentID, scheduler.DiggerJobFailed, now, now, job.ID, batchID.String(), job.DiggerJobID, scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun)
				if update.Error != nil {
					return update.Error
				}
				if update.RowsAffected != 1 {
					return fmt.Errorf("retirement lost job identity: %s", job.DiggerJobID)
				}
				job.DependencyRetiredAt, job.DependencyFailureJobID, job.Status = &now, parentID, scheduler.DiggerJobFailed
			}
		}
		allTerminal, failed := len(jobs) > 0, false
		for _, job := range jobs {
			if job.DependencyRetiredAt != nil {
				result.RetiredJobs = append(result.RetiredJobs, job.DiggerJobID)
			}
			switch job.Status {
			case scheduler.DiggerJobFailed:
				failed = true
			case scheduler.DiggerJobSucceeded:
			default:
				allTerminal = false
				result.UnresolvedJobs = append(result.UnresolvedJobs, job.DiggerJobID)
				reason := result.UnresolvedReasons[job.DiggerJobID]
				if reason == "" {
					reason = "work_pending"
				}
				if job.DispatchProtocolVersion != 1 {
					reason = "legacy_dispatch_history"
				}
				if jobHasExecutionEvidence(job) || job.Status == scheduler.DiggerJobTriggered || job.Status == scheduler.DiggerJobStarted {
					reason = "execution_active_or_uncertain"
				}
				result.UnresolvedReasons[job.DiggerJobID] = reason
			}
		}
		if !allTerminal && (batch.Status == scheduler.BatchJobFailed || batch.Status == scheduler.BatchJobSucceeded) {
			// A real workflow rerun can report started after an earlier terminal
			// result. The retired descendants stay fenced; its running parent
			// must still keep the aggregate nonterminal.
			batch.Status = scheduler.BatchJobStarted
			if err := tx.Model(&DiggerBatch{}).Where("id = ?", batchID).Update("status", batch.Status).Error; err != nil {
				return err
			}
		}
		if allTerminal && batch.Status != scheduler.BatchJobInvalidated {
			batch.Status = scheduler.BatchJobSucceeded
			if failed {
				batch.Status = scheduler.BatchJobFailed
			}
			if err := tx.Model(&DiggerBatch{}).Where("id = ?", batchID).Update("status", batch.Status).Error; err != nil {
				return err
			}
		}
		result.BatchStatus = batch.Status
		return nil
	})
	if err != nil {
		return nil, err
	}
	slog.Info("digger.dependencies.reconciled", "batch_id", batchID, "retired_jobs", result.RetiredJobs, "unresolved_jobs", result.UnresolvedJobs, "batch_status", result.BatchStatus)
	return result, nil
}

func jobHasExecutionEvidence(job DiggerJob) bool {
	return job.WorkflowDispatchStartedAt != nil || job.WorkflowRunID != 0 || job.WorkflowRunAttempt != 0 ||
		(job.WorkflowRunUrl != nil && *job.WorkflowRunUrl != "" && *job.WorkflowRunUrl != "#")
}

func jobHasWorkflowLink(tx *gorm.DB, id string) (bool, error) {
	var count int64
	err := tx.Model(&GithubDiggerJobLink{}).Where("digger_job_id = ? AND (github_job_id <> 0 OR github_workflow_run_id <> 0)", id).Count(&count).Error
	return count > 0, err
}
