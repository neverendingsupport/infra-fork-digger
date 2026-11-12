package utils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"

	"github.com/diggerhq/digger/backend/logging"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/ci"
	gh "github.com/diggerhq/digger/libs/ci/github"
	orchestrator_scheduler "github.com/diggerhq/digger/libs/scheduler"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// UpdatePRCommentRealtime updates the GitHub PR comment with current job statuses
func UpdatePRCommentRealtime(gh GithubClientProvider, batch *models.DiggerBatch) error {
	if _, err := HistoricalPublication(batch); err != nil {
		return err
	}
	return models.DB.WithPublicationBarrier(batch.ID, func(db *models.Database) error { return updatePRCommentAtPublicationBoundary(db, gh, batch) })
}

func updatePRCommentAtPublicationBoundary(db *models.Database, gh GithubClientProvider, batch *models.DiggerBatch) error {
	publication, err := historicalPublication(db, batch)
	if err != nil {
		return fmt.Errorf("historical report authorization: %w", err)
	}
	var freshBatch *models.DiggerBatch
	var freshJobs []models.DiggerJob
	if publication != nil {
		freshBatch = &publication.Batch
		freshJobs = publication.Jobs
	} else {
		freshBatch, err = db.GetDiggerBatch(&batch.ID)
		if err != nil {
			return err
		}
		freshJobs, err = db.GetDiggerJobsForBatch(batch.ID)
		if err != nil {
			return err
		}
	}
	if freshBatch.CommentId == nil || len(freshJobs) == 0 {
		return nil
	}
	prService, err := getPrServiceFromBatch(db, freshBatch, gh)
	if err != nil {
		return err
	}
	// Generate comment message with fresh job data
	message, err := GenerateRealtimeCommentMessage(freshJobs, freshBatch.BatchType)
	if err != nil {
		slog.Error("Error generating real-time comment message", "batchId", freshBatch.ID, "error", err)
		return fmt.Errorf("error generating comment message: %v", err)
	}

	// This report already has a remote ID. An uncertain edit must not fall
	// back to creating an untracked comment on every recovery retry.
	err = prService.EditComment(freshBatch.PrNumber, strconv.FormatInt(*freshBatch.CommentId, 10), message)
	if err != nil {
		return fmt.Errorf("edit existing batch report: %w", err)
	}
	slog.Debug("Successfully updated real-time PR comment", "batchId", freshBatch.ID, "commentId", *freshBatch.CommentId)

	return nil
}

// UpdatePRComment updates the PR comment for a job status change
func UpdatePRComment(gh GithubClientProvider, jobId string, job *models.DiggerJob, status string) {
	jobId = logging.SafeLogValue(jobId)
	status = logging.SafeLogValue(status)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered from panic in UpdatePRComment goroutine",
				"jobId", jobId,
				"status", status,
				"error", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	err := UpdatePRCommentRealtime(gh, job.Batch)
	if err != nil {
		slog.Warn("Failed to update PR comment for job",
			"jobId", jobId,
			"batchId", job.Batch.ID,
			"status", status,
			"error", err,
		)
	}
}

// GenerateRealtimeCommentMessage creates the markdown table for real-time PR comments
// This matches the exact format used by the CLI's BasicCommentUpdater
func GenerateRealtimeCommentMessage(jobs []models.DiggerJob, batchType orchestrator_scheduler.DiggerCommand) (string, error) {
	if len(jobs) == 0 {
		return "", fmt.Errorf("no jobs provided")
	}

	jobTypeTitle := cases.Title(language.AmericanEnglish).String(string(batchType))

	// Match exact CLI format - no header, just the table
	message := ""
	message += fmt.Sprintf("| Project | Status | %s | + | ~ | - |\n", jobTypeTitle)
	message += fmt.Sprintf("|---------|--------|------|---|---|---|\n")

	for _, job := range jobs {
		prCommentUrl := job.PRCommentUrl
		if prCommentUrl == "" {
			prCommentUrl = "#"
		}

		// Safe handling of WorkflowRunUrl pointer

		var checkRunUrl = "#"
		if job.CheckRunUrl != nil {
			checkRunUrl = *job.CheckRunUrl
		}

		workflowRunUrl := "#"
		if job.WorkflowRunUrl != nil {
			workflowRunUrl = *job.WorkflowRunUrl
		}

		// Get project name from job spec
		var jobSpec orchestrator_scheduler.JobJson
		projectDisplayName := "Unknown"
		if job.SerializedJobSpec != nil {
			err := json.Unmarshal(job.SerializedJobSpec, &jobSpec)
			if err == nil {
				// Use alias if available, fallback to project name
				if jobSpec.ProjectAlias != "" {
					projectDisplayName = jobSpec.ProjectAlias
				} else {
					projectDisplayName = jobSpec.ProjectName
				}
			} else {
				slog.Warn("Failed to unmarshal job spec for project name",
					"jobId", job.DiggerJobID,
					"error", err)
			}
		}

		// Default resource counts to 0 if DiggerJobSummary is nil
		resourcesCreated := uint(0)
		resourcesUpdated := uint(0)
		resourcesDeleted := uint(0)

		// Only access DiggerJobSummary fields if it's not nil
		if job.DiggerJobSummary.ID != 0 {
			resourcesCreated = job.DiggerJobSummary.ResourcesCreated
			resourcesUpdated = job.DiggerJobSummary.ResourcesUpdated
			resourcesDeleted = job.DiggerJobSummary.ResourcesDeleted
		}

		if job.DependencyRetiredAt != nil {
			message += fmt.Sprintf("|:no_entry_sign: **%s** |skipped: %s | <a href='%s'>%s</a> | 0 | 0 | 0|\n", projectDisplayName, job.DependencySkipReason(), checkRunUrl, jobTypeTitle)
			continue
		}

		// Match exact CLI format: |emoji **project** |<a href='workflow'>status</a> | <a href='comment'>jobType</a> | + | ~ | - |
		message += fmt.Sprintf("|%s **%s** |<a href='%s'>%s</a> | <a href='%s'>%s</a> | %d | %d | %d|\n",
			job.Status.ToEmoji(),
			projectDisplayName,
			workflowRunUrl,
			job.Status.ToString(),
			checkRunUrl,
			jobTypeTitle,
			resourcesCreated,
			resourcesUpdated,
			resourcesDeleted)
	}

	// Add instruction helpers (same as CLI)
	message += "\n" + formatExampleCommands()

	// Handle comment length limits
	const GithubCommentMaxLength = 65536
	if len(message) > GithubCommentMaxLength {
		slog.Warn("Comment message too long, trimming", "originalLength", len(message), "maxLength", GithubCommentMaxLength)
		const footer = "\n\n[Message truncated due to length limits]"
		trimLength := len(message) - GithubCommentMaxLength + len(footer)
		message = message[:len(message)-trimLength] + footer
		slog.Debug("Trimmed comment message", "newLength", len(message))
	}

	return message, nil
}

// formatExampleCommands creates a collapsible markdown section with example commands
// This matches the exact format used by the CLI's BasicCommentUpdater
func formatExampleCommands() string {
	return `
<details>
  <summary>Instructions</summary>

⏩ To apply these changes, run the following command:

` + "```" + `bash
digger apply
` + "```" + `

🚮 To unlock the projects in this PR run the following command:
` + "```" + `bash
digger unlock
` + "```" + `
</details>
`
}

// UpdateOrCreateSummaryComment updates or creates the summary comment for the batch
func UpdateOrCreateSummaryComment(prService ci.PullRequestService, batch *models.DiggerBatch, message string) (*int64, error) {
	if _, err := HistoricalPublication(batch); err != nil {
		return nil, err
	}
	if models.DB == nil {
		return updateSummaryAtPublicationBoundary(nil, prService, batch, message)
	}
	var result *int64
	err := models.DB.WithPublicationBarrier(batch.ID, func(db *models.Database) error {
		var err error
		result, err = updateSummaryAtPublicationBoundary(db, prService, batch, message)
		return err
	})
	return result, err
}

func updateSummaryAtPublicationBoundary(db *models.Database, prService ci.PullRequestService, batch *models.DiggerBatch, message string) (*int64, error) {
	publication, err := historicalPublication(db, batch)
	if err != nil {
		return nil, err
	}
	if publication != nil {
		service, ok := prService.(*gh.GithubService)
		if !ok || service.Owner != publication.Batch.RepoOwner || service.RepoName != publication.Batch.RepoName {
			return nil, models.ErrDependencyStopConflict
		}
		batch = &publication.Batch
		message, err = GenerateRealtimeCommentMessage(publication.Jobs, batch.BatchType)
		if err != nil {
			return nil, err
		}
		err = prService.EditComment(batch.PrNumber, strconv.FormatInt(*batch.CommentId, 10), message)
		return batch.CommentId, err
	}

	if batch.CommentId != nil {
		// Update existing comment
		commentIdStr := strconv.FormatInt(*batch.CommentId, 10)
		err := prService.EditComment(batch.PrNumber, commentIdStr, message)
		if err != nil {
			slog.Warn("Failed to update existing comment, will create new one", "commentId", *batch.CommentId, "prNumber", batch.PrNumber, "error", err)
			// Fall through to create new comment
		} else {
			slog.Debug("Successfully updated existing comment", "commentId", *batch.CommentId, "prNumber", batch.PrNumber)
			return batch.CommentId, nil
		}
	}

	// Create new comment
	comment, err := prService.PublishComment(batch.PrNumber, message)
	if err != nil {
		slog.Error("Failed to create new comment", "prNumber", batch.PrNumber, "error", err)
		return nil, fmt.Errorf("failed to create comment: %v", err)
	}

	commentId, err := strconv.ParseInt(comment.Id, 10, 64)
	if err != nil {
		slog.Error("Failed to parse comment ID", "commentIdStr", comment.Id, "error", err)
		return nil, fmt.Errorf("failed to parse comment ID: %v", err)
	}

	slog.Debug("Successfully created new comment", "commentId", commentId, "prNumber", batch.PrNumber)
	return &commentId, nil
}

// GetPrServiceFromBatch gets the appropriate PR service for a batch
func GetPrServiceFromBatch(batch *models.DiggerBatch, gh GithubClientProvider) (ci.PullRequestService, error) {
	return getPrServiceFromBatch(models.DB, batch, gh)
}

func getPrServiceFromBatch(db *models.Database, batch *models.DiggerBatch, gh GithubClientProvider) (ci.PullRequestService, error) {
	slog.Debug("Getting PR service for batch",
		"batchId", batch.ID,
		"vcs", batch.VCS,
		"prNumber", batch.PrNumber,
	)

	switch batch.VCS {
	case "github":
		slog.Debug("Using GitHub service for batch",
			"batchId", batch.ID,
			"installationId", batch.GithubInstallationId,
			"repoFullName", batch.RepoFullName,
		)

		service, _, err := getGithubService(
			db,
			gh,
			batch.GithubInstallationId,
			batch.RepoFullName,
			batch.RepoOwner,
			batch.RepoName,
		)

		if err != nil {
			slog.Error("Error getting GitHub service",
				"batchId", batch.ID,
				"repoFullName", batch.RepoFullName,
				"error", err,
			)
		} else {
			slog.Debug("Successfully got GitHub service",
				"batchId", batch.ID,
				"repoFullName", batch.RepoFullName,
			)
		}

		return service, err

	case "gitlab":
		slog.Debug("Using GitLab service for batch",
			"batchId", batch.ID,
			"projectId", batch.GitlabProjectId,
			"repoFullName", batch.RepoFullName,
		)

		service, err := GetGitlabService(
			GitlabClientProvider{},
			batch.GitlabProjectId,
			batch.RepoName,
			batch.RepoFullName,
			batch.PrNumber,
			"",
		)

		if err != nil {
			slog.Error("Error getting GitLab service",
				"batchId", batch.ID,
				"projectId", batch.GitlabProjectId,
				"error", err,
			)
		} else {
			slog.Debug("Successfully got GitLab service",
				"batchId", batch.ID,
				"projectId", batch.GitlabProjectId,
			)
		}

		return service, err
	}

	slog.Error("Unsupported VCS type", "vcs", batch.VCS, "batchId", batch.ID)
	return nil, fmt.Errorf("could not retrieve a service for %v", batch.VCS)
}
