package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
)

var ErrDependencyStopProviderUnavailable = errors.New("historical stop provider evidence is unavailable")
var ErrDependencyStopActionsActive = errors.New("repository Actions have not all stopped")

// CheckDependencyStopSource reads the original command and its named failed
// prerequisite. It does not assert whether the legacy child ever executed.
func CheckDependencyStopSource(ctx context.Context, client *github.Client, batch *models.DiggerBatch, request models.DependencyStopRequest, repositoryID int64) (*models.DependencyStopVerification, error) {
	return checkDependencyStopSource(ctx, client, batch, request, repositoryID, false)
}

func checkDependencyStopSource(ctx context.Context, client *github.Client, batch *models.DiggerBatch, request models.DependencyStopRequest, repositoryID int64, original bool) (*models.DependencyStopVerification, error) {
	started := time.Now().UTC()
	comment, _, err := client.Issues.GetComment(ctx, batch.RepoOwner, batch.RepoName, request.SourceCommentID)
	if err != nil || comment == nil {
		return nil, ErrDependencyStopProviderUnavailable
	}
	issueURL, err := url.Parse(comment.GetIssueURL())
	body := sha256.Sum256([]byte(comment.GetBody()))
	if err != nil || issueURL.Scheme != "https" || issueURL.Path != "/repos/"+batch.RepoFullName+"/issues/"+strconv.Itoa(request.PRNumber) || comment.GetID() != request.SourceCommentID || hex.EncodeToString(body[:]) != request.BodySHA256 {
		return nil, ErrDependencyStopProviderUnavailable
	}
	pr, _, err := client.PullRequests.Get(ctx, batch.RepoOwner, batch.RepoName, request.PRNumber)
	if err != nil || pr == nil || pr.Base == nil || pr.Base.Repo == nil || pr.Head == nil || pr.Head.GetSHA() == "" || pr.GetNumber() != request.PRNumber || pr.Base.Repo.GetID() != repositoryID || pr.Base.Repo.GetFullName() != batch.RepoFullName || !pr.GetMerged() || pr.GetState() != "closed" || pr.GetMergeCommitSHA() == "" {
		return nil, ErrDependencyStopProviderUnavailable
	}
	var run *github.WorkflowRun
	if original {
		run, _, err = client.Actions.GetWorkflowRunAttempt(ctx, batch.RepoOwner, batch.RepoName, request.FailedPrerequisite.WorkflowRunID, int(request.FailedPrerequisite.WorkflowRunAttempt), nil)
	} else {
		run, _, err = client.Actions.GetWorkflowRunByID(ctx, batch.RepoOwner, batch.RepoName, request.FailedPrerequisite.WorkflowRunID)
	}
	if err != nil || run == nil || run.Repository == nil || run.Repository.GetID() != repositoryID || run.Repository.GetFullName() != batch.RepoFullName || run.GetID() != request.FailedPrerequisite.WorkflowRunID || int64(run.GetRunAttempt()) != request.FailedPrerequisite.WorkflowRunAttempt || run.GetStatus() != "completed" || run.GetConclusion() != "failure" {
		return nil, ErrDependencyStopProviderUnavailable
	}
	return &models.DependencyStopVerification{CurrentParentRunID: run.GetID(), CurrentParentAttempt: int64(run.GetRunAttempt()), CurrentParentStatus: scheduler.DiggerJobFailed, CurrentParentProviderStatus: run.GetStatus(), CurrentParentConclusion: run.GetConclusion(), Version: 1, RepositoryID: repositoryID, RepoFullName: batch.RepoFullName, PRNumber: request.PRNumber, Merged: true, PRState: "closed", PRHeadSHA: pr.Head.GetSHA(), MergeCommitSHA: pr.GetMergeCommitSHA(), SourceCommentID: request.SourceCommentID, BodySHA256: request.BodySHA256, ParentRunID: run.GetID(), ParentAttempt: int64(run.GetRunAttempt()), ParentStatus: run.GetStatus(), ParentConclusion: run.GetConclusion(), StartedAt: started, FinishedAt: time.Now().UTC()}, nil
}

// VerifyDependencyStopQuiescence accepts only complete zero-result searches for
// every documented incomplete Actions status, without head/branch/age filtering.
func VerifyDependencyStopQuiescence(ctx context.Context, client *github.Client, batch *models.DiggerBatch, request models.DependencyStopRequest, repositoryID int64) (*models.DependencyStopVerification, error) {
	return verifyDependencyStopChecks(ctx, client, batch, func() (*models.DependencyStopVerification, error) {
		return CheckDependencyStopSource(ctx, client, batch, request, repositoryID)
	})
}

func VerifyDependencyStopCheckpoint(ctx context.Context, client *github.Client, batch *models.DiggerBatch, view *models.DependencyStopView) (*models.DependencyStopVerification, error) {
	return verifyDependencyStopChecks(ctx, client, batch, func() (*models.DependencyStopVerification, error) {
		request := view.Operation.Request
		evidence, err := checkDependencyStopSource(ctx, client, batch, request, view.Operation.Snapshot.Origin.RepositoryID, true)
		if err != nil {
			return nil, err
		}
		for _, job := range view.Snapshot.Jobs {
			if job.DBID == request.FailedPrerequisite.DBID && job.JobID == request.FailedPrerequisite.JobID {
				if (job.Status != scheduler.DiggerJobFailed && job.Status != scheduler.DiggerJobSucceeded) || job.RunID <= 0 || job.RunAttempt <= 0 {
					return nil, ErrDependencyStopActionsActive
				}
				run, _, err := client.Actions.GetWorkflowRunByID(ctx, batch.RepoOwner, batch.RepoName, job.RunID)
				if err != nil || run == nil || run.Repository == nil || run.Repository.GetID() != evidence.RepositoryID || run.Repository.GetFullName() != batch.RepoFullName || run.GetID() != job.RunID || int64(run.GetRunAttempt()) != job.RunAttempt || run.GetStatus() != "completed" || run.GetConclusion() == "" {
					return nil, ErrDependencyStopProviderUnavailable
				}
				evidence.CurrentParentRunID = job.RunID
				evidence.CurrentParentAttempt = job.RunAttempt
				evidence.CurrentParentStatus = job.Status
				evidence.CurrentParentProviderStatus = run.GetStatus()
				evidence.CurrentParentConclusion = run.GetConclusion()
				return evidence, nil
			}
		}
		return nil, ErrDependencyStopProviderUnavailable
	})
}

func verifyDependencyStopChecks(ctx context.Context, client *github.Client, batch *models.DiggerBatch, check func() (*models.DependencyStopVerification, error)) (*models.DependencyStopVerification, error) {
	first, err := check()
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, status := range models.DependencyStopIncompleteStatuses {
		runs, response, err := client.Actions.ListRepositoryWorkflowRuns(ctx, batch.RepoOwner, batch.RepoName, &github.ListWorkflowRunsOptions{Status: status, ListOptions: github.ListOptions{PerPage: 100, Page: 1}})
		if err != nil || response == nil || runs == nil || runs.TotalCount == nil {
			return nil, ErrDependencyStopProviderUnavailable
		}
		if runs.GetTotalCount() > 0 || len(runs.WorkflowRuns) > 0 {
			return nil, ErrDependencyStopActionsActive
		}
		if runs.GetTotalCount() != 0 || response.NextPage != 0 || response.PrevPage != 0 || response.LastPage > 1 {
			return nil, ErrDependencyStopProviderUnavailable
		}
		counts[status] = 0
	}
	last, err := check()
	if err != nil {
		return nil, err
	}
	if last.MergeCommitSHA != first.MergeCommitSHA || last.PRHeadSHA != first.PRHeadSHA || last.CurrentParentRunID != first.CurrentParentRunID || last.CurrentParentAttempt != first.CurrentParentAttempt || last.CurrentParentStatus != first.CurrentParentStatus || last.CurrentParentConclusion != first.CurrentParentConclusion {
		return nil, ErrDependencyStopProviderUnavailable
	}
	last.StartedAt = first.StartedAt
	last.ActiveCounts, last.ActionsComplete = counts, true
	last.FinishedAt = time.Now().UTC()
	return last, nil
}
