package models

import (
	"errors"
	"log/slog"
	"slices"

	"github.com/diggerhq/digger/libs/scheduler"
	"gorm.io/gorm"
)

// CommandProvenance describes a bounded, exhaustive query, not authorization to
// apply. Consumers must bind command creation to recovery, verify successful
// current-head jobs and live GitHub attempts, and recheck recovery at admission.
type CommandProvenance struct {
	Version  int                      `json:"version"`
	Complete bool                     `json:"complete"`
	Batches  []CommandProvenanceBatch `json:"batches"`
}

type CommandProvenanceBatch struct {
	BatchID          string                      `json:"batch_id"`
	Origin           CommandProvenanceOrigin     `json:"origin"`
	AggregateCheckID string                      `json:"aggregate_check_id"`
	ManifestComplete bool                        `json:"manifest_complete"`
	Status           scheduler.DiggerBatchStatus `json:"status"`
	Jobs             []CommandProvenanceJob      `json:"jobs"`
}

type CommandProvenanceOrigin struct {
	RepositoryID    int64                   `json:"repository_id"`
	RepoFullName    string                  `json:"repo_full_name"`
	PrNumber        int                     `json:"pr_number"`
	SourceCommentID int64                   `json:"source_comment_id"`
	BodySHA256      string                  `json:"body_sha256"`
	CommitSHA       string                  `json:"commit_sha"`
	Command         scheduler.DiggerCommand `json:"command"`
}

type CommandProvenanceJob struct {
	DBID               uint                      `json:"db_id"`
	JobID              string                    `json:"job_id"`
	Project            string                    `json:"project"`
	ProjectCheckID     string                    `json:"project_check_id"`
	Status             scheduler.DiggerJobStatus `json:"status"`
	WorkflowURL        string                    `json:"workflow_url"`
	WorkflowRunID      int64                     `json:"workflow_run_id"`
	WorkflowRunAttempt int64                     `json:"workflow_run_attempt"`
}

func (db *Database) GetCommandProvenance(orgID uint, repoFullName string, prNumber int, sourceCommentID int64) (*CommandProvenance, error) {
	if orgID == 0 || repoFullName == "" || prNumber <= 0 || sourceCommentID <= 0 {
		return nil, errors.New("command provenance requires organisation, repository, PR and source comment")
	}
	response := &CommandProvenance{Version: 1, Complete: true, Batches: make([]CommandProvenanceBatch, 0)}
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		var origins []BatchCommandOrigin
		// Scope the immutable installation snapshot, not mutable batch/report IDs.
		// EXISTS avoids duplicating origins if legacy links contain duplicates.
		err := tx.Where("repo_full_name = ? AND pr_number = ? AND source_comment_id = ?", repoFullName, prNumber, sourceCommentID).
			Where(`EXISTS (SELECT 1 FROM github_app_installation_links l
				JOIN organisations o ON o.id = l.organisation_id
				WHERE l.github_installation_id = batch_command_origins.github_installation_id
				AND o.id = ? AND l.status = ? AND l.deleted_at IS NULL AND o.deleted_at IS NULL)`, orgID, GithubAppInstallationLinkActive).
			Order("batch_id ASC").Limit(MaxCommandProvenanceBatches + 1).Find(&origins).Error
		if err != nil {
			return err
		}
		if len(origins) > MaxCommandProvenanceBatches {
			return ErrProvenanceTooLarge
		}
		for _, origin := range origins {
			batch, err := commandProvenanceBatch(tx, origin)
			if err != nil {
				return err
			}
			response.Batches = append(response.Batches, batch)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slog.Debug("digger.command_provenance.queried", "org_id", orgID, "repo", repoFullName, "pr_number", prNumber, "source_comment_id", sourceCommentID, "batch_count", len(response.Batches))
	return response, nil
}

func commandProvenanceBatch(tx *gorm.DB, origin BatchCommandOrigin) (CommandProvenanceBatch, error) {
	result := CommandProvenanceBatch{
		BatchID: origin.BatchID.String(), AggregateCheckID: origin.AggregateCheckID,
		Origin: CommandProvenanceOrigin{
			RepositoryID: origin.RepositoryID, RepoFullName: origin.RepoFullName,
			PrNumber: origin.PrNumber, SourceCommentID: origin.SourceCommentID,
			BodySHA256: origin.BodySHA256, CommitSHA: origin.CommitSHA, Command: origin.Command,
		},
		Jobs: make([]CommandProvenanceJob, 0),
	}
	if len(origin.JobManifest) > MaxCommandProvenanceJobs || len(origin.ExpectedProjects) > MaxCommandProvenanceJobs {
		return result, ErrProvenanceTooLarge
	}
	var batch DiggerBatch
	err := tx.First(&batch, "id = ?", origin.BatchID).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return result, err
	}
	batchMatches := err == nil && commandOriginMatchesBatch(origin, batch)
	if batchMatches {
		result.Status = batch.Status
	}
	jobs, err := commandOriginJobs(tx, origin.BatchID)
	if err != nil {
		return result, err
	}
	currentManifest, manifestErr := commandOriginManifest(origin, jobs)
	result.ManifestComplete = origin.Sealed && batchMatches && manifestErr == nil && slices.Equal(currentManifest, origin.JobManifest)
	if !result.ManifestComplete {
		slog.Warn("digger.command_provenance.manifest_incomplete", "batch_id", origin.BatchID, "sealed", origin.Sealed, "batch_matches", batchMatches, "current_job_count", len(jobs), "manifest_job_count", len(origin.JobManifest))
	}
	// Return every immutable manifest entry. A missing or changed identity has
	// unknown status/attempt, never another job's successful result. Extra jobs
	// invalidate completeness above; they cannot silently disappear as evidence.
	byID := make(map[uint]DiggerJob, len(jobs))
	for _, job := range jobs {
		byID[job.ID] = job
	}
	for _, entry := range origin.JobManifest {
		out := CommandProvenanceJob{DBID: entry.DBID, JobID: entry.JobID, Project: entry.Project, ProjectCheckID: entry.ProjectCheckID}
		job, exists := byID[entry.DBID]
		if exists && batchMatches && commandProvenanceJobMatches(origin, entry, job) {
			out.Status = job.Status
			out.WorkflowURL = commandOriginString(job.WorkflowRunUrl)
			// Legacy zero values are explicitly unknown, even when a URL exists.
			out.WorkflowRunID = job.WorkflowRunID
			out.WorkflowRunAttempt = job.WorkflowRunAttempt
		}
		result.Jobs = append(result.Jobs, out)
	}
	return result, nil
}

func commandProvenanceJobMatches(origin BatchCommandOrigin, entry BatchCommandOriginJob, job DiggerJob) bool {
	// Reuse the same checks as sealing while keeping each independently matching
	// entry readable when a different sibling is missing or has changed.
	partial := origin
	partial.ExpectedProjects = []string{entry.Project}
	manifest, err := commandOriginManifest(partial, []DiggerJob{job})
	return err == nil && len(manifest) == 1 && manifest[0] == entry
}
