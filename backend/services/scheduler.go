package services

import (
	"errors"
	"fmt"
	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	orchestrator_scheduler "github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"log/slog"
)

func DiggerJobCompleted(client *github.Client, batchId *uuid.UUID, parentJob *models.DiggerJob, repoFullName string, repoOwner string, repoName string, workflowFileName string, gh utils.GithubClientProvider) error {
	slog.Info("Job completed", "parentJobId", parentJob.DiggerJobID)

	jobLinksForParent, err := models.DB.GetDiggerJobParentLinksByParentId(&parentJob.DiggerJobID)
	if err != nil {
		slog.Error("Failed to get parent job links", "parentJobId", parentJob.DiggerJobID, "error", err)
		return err
	}

	var schedulingErrors []error
	for _, jobLink := range jobLinksForParent {
		jobLinksForChild, err := models.DB.GetDiggerJobParentLinksChildId(&jobLink.DiggerJobId)
		if err != nil {
			slog.Error("Failed to get child job links", "childJobId", jobLink.DiggerJobId, "error", err)
			return err
		}
		allParentJobsAreComplete := true

		for _, jobLinkForChild := range jobLinksForChild {
			parentJob, err := models.DB.GetDiggerJob(jobLinkForChild.ParentDiggerJobId)
			if err != nil {
				slog.Error("Failed to get parent job", "parentJobId", jobLinkForChild.ParentDiggerJobId, "error", err)
				return err
			}

			if parentJob.Status != orchestrator_scheduler.DiggerJobSucceeded {
				allParentJobsAreComplete = false
				break
			}
		}

		if allParentJobsAreComplete {
			job, err := models.DB.GetDiggerJob(jobLink.DiggerJobId)
			if err != nil {
				slog.Error("Failed to get job", "jobId", jobLink.DiggerJobId, "error", err)
				return err
			}

			slog.Info("All parent jobs completed, scheduling job",
				"jobId", job.DiggerJobID,
				slog.Group("repository",
					slog.String("fullName", repoFullName),
					slog.String("owner", repoOwner),
					slog.String("name", repoName),
				),
			)

			ciBackend := ci_backends.GithubActionCi{Client: client}
			if err := ScheduleJob(ciBackend, repoFullName, repoOwner, repoName, batchId, job, gh); err != nil {
				// An uncertain dispatch keeps its own claim, but must not strand
				// independently ready siblings whose parents have all completed.
				schedulingErrors = append(schedulingErrors, fmt.Errorf("schedule job %s: %w", job.DiggerJobID, err))
			}
		}
	}
	return errors.Join(schedulingErrors...)
}

func ScheduleJob(ciBackend ci_backends.CiBackend, repoFullname string, repoOwner string, repoName string, batchId *uuid.UUID, job *models.DiggerJob, gh utils.GithubClientProvider) error {
	maxConcurrencyForBatch := config.DiggerConfig.GetInt("max_concurrency_per_batch")

	if maxConcurrencyForBatch == 0 {
		// concurrency limits not set
		slog.Info("Scheduling job without concurrency limit", "jobId", job.DiggerJobID, "batchId", batchId)
		err := TriggerJob(gh, ciBackend, repoFullname, repoOwner, repoName, batchId, job)
		if err != nil {
			slog.Error("Could not trigger job", "jobId", job.DiggerJobID, "error", err)
			return err
		}
	} else {
		// concurrency limits set
		slog.Info("Scheduling job with concurrency limit",
			"jobId", job.DiggerJobID,
			"batchId", batchId,
			"maxConcurrency", maxConcurrencyForBatch)

		jobCount, err := models.DB.CountActiveDiggerJobDispatches(batchId.String())
		if err != nil {
			slog.Error("Failed to get jobs for batch", "batchId", batchId, "error", err)
			return err
		}

		slog.Debug("Current running jobs for batch", "count", jobCount, "batchId", batchId)

		if jobCount >= int64(maxConcurrencyForBatch) {
			slog.Info("Maximum concurrency reached, queueing job",
				"jobId", job.DiggerJobID,
				"currentJobCount", jobCount,
				"maxConcurrency", maxConcurrencyForBatch)

			queued, err := models.DB.QueueDiggerJobIfCreated(job)
			if err != nil {
				return err
			}
			if !queued {
				slog.Info("Job already queued or dispatched", "jobId", job.DiggerJobID)
			}
			return nil
		} else {
			err := TriggerJob(gh, ciBackend, repoFullname, repoOwner, repoName, batchId, job)
			if err != nil {
				slog.Error("Could not trigger job", "jobId", job.DiggerJobID, "error", err)
				return err
			}
		}
	}
	return nil
}

func TriggerJob(gh utils.GithubClientProvider, ciBackend ci_backends.CiBackend, repoFullname string, repoOwner string, repoName string, batchId *uuid.UUID, job *models.DiggerJob) error {
	slog.Info("Triggering job",
		"jobId", job.DiggerJobID,
		slog.Group("repository",
			slog.String("fullName", repoFullname),
			slog.String("owner", repoOwner),
			slog.String("name", repoName),
		),
	)

	if job.SerializedJobSpec == nil {
		slog.Error("Job spec is nil", "jobId", job.DiggerJobID)
		return fmt.Errorf("JobSpec is nil, skipping")
	}

	jobString := string(job.SerializedJobSpec)
	slog.Debug("Job specification", "jobId", job.DiggerJobID, "jobSpec", jobString)

	runName, err := GetRunNameFromJob(*job)
	if err != nil {
		slog.Error("Could not get run name", "jobId", job.DiggerJobID, "error", err)
		return fmt.Errorf("could not get run name %v", err)
	}

	spec, err := GetSpecFromJob(*job)
	if err != nil {
		slog.Error("Could not get spec", "jobId", job.DiggerJobID, "error", err)
		return fmt.Errorf("could not get spec %v", err)
	}

	vcsToken, err := GetVCSTokenFromJob(*job, gh)
	if err != nil {
		slog.Error("Could not get VCS token", "jobId", job.DiggerJobID, "error", err)
		return fmt.Errorf("could not get vcs token: %v", err)
	}

	// Competing parent callbacks and queued-job sweepers can prepare the same
	// job. Only the first committed dispatch claim may send; later status
	// reports cannot reopen that claim.
	claimed, err := models.DB.ClaimDiggerJobDispatch(job)
	if err != nil {
		return fmt.Errorf("claim workflow dispatch: %w", err)
	}
	if !claimed {
		slog.Info("Workflow dispatch already claimed", "jobId", job.DiggerJobID)
		return nil
	}

	err = ciBackend.TriggerWorkflow(*spec, *runName, *vcsToken)
	if err != nil {
		slog.Error("Workflow dispatch response unavailable; retaining claim", "jobId", job.DiggerJobID, "error", err)
		return err
	}

	// A fast workflow callback may already have advanced this job. Never save
	// the stale pre-dispatch object over that newer status or its run identity.
	slog.Info("Job successfully triggered", "jobId", job.DiggerJobID)

	return nil
}
