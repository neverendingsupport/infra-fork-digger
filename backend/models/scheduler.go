package models

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	orchestrator_scheduler "github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type ImpactedProject struct {
	gorm.Model
	ID           uuid.UUID `gorm:"primary_key"`
	RepoFullName string    `gorm:"index:idx_org_repo"`
	CommitSha    string    `gorm:"index:idx_org_repo"`
	PrNumber     *int
	Branch       *string
	ProjectName  string
	Planned      bool
	Applied      bool
}

type DiggerJobParentLink struct {
	gorm.Model
	DiggerJobId       string `gorm:"size:50,index:idx_digger_job_id"`
	ParentDiggerJobId string `gorm:"index:idx_parent_digger_job_id"`
}

type DiggerVCSType string

const DiggerVCSGithub DiggerVCSType = "github"
const DiggerVCSGitlab DiggerVCSType = "gitlab"
const DiggerVCSBitbucket DiggerVCSType = "bitbucket"

type DiggerBatch struct {
	gorm.Model
	ID                       uuid.UUID `gorm:"primary_key"`
	DiggerBatchID            string    `gorm:"size:20,index:idx_digger_batch_id"` // shorter version of the ID to be able to use in check run
	Layer                    uint
	VCS                      DiggerVCSType
	PrNumber                 int    `gorm:"index:idx_digger_batches_pr_commit,priority:2"`
	CommitSha                string `gorm:"index:idx_digger_batches_pr_commit,priority:3"`
	CommentId                *int64
	CheckRunId               *string
	CheckRunUrl              *string
	AiSummaryCommentId       string
	Status                   orchestrator_scheduler.DiggerBatchStatus
	BranchName               string
	DiggerConfig             string
	GithubInstallationId     int64
	GitlabProjectId          int
	RepoFullName             string `gorm:"index:idx_digger_batches_pr_commit,priority:1"`
	RepoOwner                string
	RepoName                 string
	BatchType                orchestrator_scheduler.DiggerCommand
	ReportTerraformOutputs   bool
	CoverAllImpactedProjects bool
	// used for module source grouping comments
	SourceDetails   []byte
	VCSConnectionId *uint ``
	VCSConnection   VCSConnection

	// Only snapshots prepared by HistoricalPublication carry this local content guard.
	historicalPublicationSHA256 string
}

type DiggerJob struct {
	gorm.Model
	DiggerJobID                  string                                 `gorm:"size:50,index:idx_digger_job_id"`
	Status                       orchestrator_scheduler.DiggerJobStatus `gorm:"index:idx_digger_jobs_status"`
	RunName                      string
	ProjectName                  string
	Batch                        *DiggerBatch
	BatchID                      *string `gorm:"index:idx_digger_job_id"`
	PRCommentUrl                 string
	PRCommentId                  *int64
	CheckRunId                   *string
	CheckRunUrl                  *string
	DiggerJobSummary             DiggerJobSummary
	DiggerJobSummaryID           uint
	SerializedJobSpec            []byte
	SerializedReporterSpec       []byte
	SerializedCommentUpdaterSpec []byte
	SerializedLockSpec           []byte
	SerializedBackendSpec        []byte
	SerializedVcsSpec            []byte
	SerializedPolicySpec         []byte
	SerializedVariablesSpec      []byte
	TerraformOutput              string
	// represents a footprint of terraform plan json for similarity checks
	PlanFootprint      []byte
	WorkflowFile       string
	WorkflowRunUrl     *string
	WorkflowRunID      int64
	WorkflowRunAttempt int64
	// Written by dispatch claims and runner observations. Stale whole-row saves
	// must not erase permission already consumed by a possible dispatch.
	WorkflowDispatchStartedAt *time.Time `gorm:"<-:false"`
	CallbackRevision          int64      `gorm:"not null;default:0;<-:false"`
	// Version zero is legacy: absence of a dispatch claim is not historical proof.
	DispatchProtocolVersion         int             `gorm:"not null;default:0;<-:create"`
	DependencyRetiredAt             *time.Time      `gorm:"<-:false"`
	DependencyFailureJobID          string          `gorm:"not null;default:'';<-:false"`
	DependencyRetirementOperationID *uuid.UUID      `gorm:"type:uuid;<-:false"`
	DependencyStop                  *DependencyStop `gorm:"-"`
	StatusUpdatedAt                 time.Time
	ReporterType                    string `gorm:"default:'lazy'"` // temporary, to be replaced by SerializedReporterSpec
}

type DiggerJobSummary struct {
	gorm.Model
	ResourcesCreated uint
	ResourcesDeleted uint
	ResourcesUpdated uint
}

// These tokens will be pre
type JobToken struct {
	gorm.Model
	Value          string `gorm:"uniqueJobTokenIndex:idx_token"`
	Expiry         time.Time
	OrganisationID uint
	Organisation   Organisation
	Type           string // AccessTokenType starts with j:
}

type DiggerJobLinkStatus int8

const (
	DiggerJobLinkCreated   DiggerJobLinkStatus = 1
	DiggerJobLinkSucceeded DiggerJobLinkStatus = 2
)

// GithubDiggerJobLink links GitHub Workflow Job id to Digger's Job Id
type GithubDiggerJobLink struct {
	gorm.Model
	DiggerJobId         string `gorm:"size:50,index:idx_digger_job_id"`
	RepoFullName        string
	GithubJobId         int64 `gorm:"index:idx_github_job_id"`
	GithubWorkflowRunId int64
	Status              DiggerJobLinkStatus
}

func (j *DiggerJob) MapToJsonStruct() (orchestrator_scheduler.SerializedJob, error) {
	var job orchestrator_scheduler.JobJson
	err := json.Unmarshal(j.SerializedJobSpec, &job)
	if err != nil {
		slog.Error("Failed to unmarshal serialized job spec", "jobId", j.DiggerJobID, "error", err)
		return orchestrator_scheduler.SerializedJob{}, err
	}

	serialized := orchestrator_scheduler.SerializedJob{
		DiggerJobId:      j.DiggerJobID,
		SkipReason:       j.DependencySkipReason(),
		Status:           j.Status,
		JobString:        j.SerializedJobSpec,
		PlanFootprint:    j.PlanFootprint,
		ProjectName:      job.ProjectName,
		ProjectAlias:     job.ProjectAlias,
		WorkflowRunUrl:   j.WorkflowRunUrl,
		PRCommentUrl:     j.PRCommentUrl,
		ResourcesCreated: j.DiggerJobSummary.ResourcesCreated,
		ResourcesUpdated: j.DiggerJobSummary.ResourcesUpdated,
		ResourcesDeleted: j.DiggerJobSummary.ResourcesDeleted,
	}

	slog.Debug("Mapped job to JSON struct",
		"jobId", j.DiggerJobID,
		"status", j.Status,
		"projectName", job.ProjectName)

	return serialized, nil
}

func (b *DiggerBatch) MapToJsonStruct() (orchestrator_scheduler.SerializedBatch, error) {
	res := orchestrator_scheduler.SerializedBatch{
		ID:           b.ID.String(),
		PrNumber:     b.PrNumber,
		Status:       b.Status,
		BranchName:   b.BranchName,
		RepoFullName: b.RepoFullName,
		RepoOwner:    b.RepoOwner,
		RepoName:     b.RepoName,
		BatchType:    b.BatchType,
	}

	slog.Debug("Mapping batch to JSON struct",
		"batchId", b.ID.String(),
		"repoFullName", b.RepoFullName,
		"prNumber", b.PrNumber)

	serializedJobs := make([]orchestrator_scheduler.SerializedJob, 0)
	jobs, err := DB.GetDiggerJobsForBatch(b.ID)
	if err != nil {
		slog.Error("Could not get jobs for batch", "batchId", b.ID.String(), "error", err)
		return res, fmt.Errorf("could not unmarshall digger batch: %v", err)
	}

	for _, job := range jobs {
		jobJson, err := job.MapToJsonStruct()
		if err != nil {
			slog.Error("Error mapping job to struct",
				"jobId", job.ID,
				"diggerJobId", job.DiggerJobID,
				"batchId", b.ID.String(),
				"error", err)
			return res, fmt.Errorf("error mapping job to struct (ID: %v); %v", job.ID, err)
		}
		serializedJobs = append(serializedJobs, jobJson)
	}

	res.Jobs = serializedJobs
	slog.Debug("Successfully mapped batch to JSON struct",
		"batchId", b.ID.String(),
		"jobCount", len(serializedJobs))

	return res, nil
}

func GetCommitStatusForJob(job *DiggerJob) (string, error) {
	switch job.Status {
	case orchestrator_scheduler.DiggerJobStarted:
		return "pending", nil
	case orchestrator_scheduler.DiggerJobTriggered:
		return "pending", nil
	case orchestrator_scheduler.DiggerJobCreated:
		return "pending", nil
	case orchestrator_scheduler.DiggerJobSucceeded:
		return "success", nil
	case orchestrator_scheduler.DiggerJobFailed:
		return "failed", nil
	}
	return "", fmt.Errorf("unknown job status: %v", job.Status)
}

func GetCheckRunStatusForJob(job *DiggerJob) (string, error) {
	if job.DependencyStopPending() {
		return "in_progress", nil
	}
	switch job.Status {
	case orchestrator_scheduler.DiggerJobStarted:
		return "in_progress", nil
	case orchestrator_scheduler.DiggerJobTriggered:
		return "in_progress", nil
	case orchestrator_scheduler.DiggerJobCreated:
		return "in_progress", nil
	case orchestrator_scheduler.DiggerJobQueuedForRun:
		return "queued", nil
	case orchestrator_scheduler.DiggerJobSucceeded:
		return "completed", nil
	case orchestrator_scheduler.DiggerJobFailed:
		return "completed", nil
	}
	slog.Error("Unknown job status in GetCheckRunStatusForJob - this will cause GitHub API 422 error",
		"jobId", job.DiggerJobID,
		"jobStatus", job.Status,
		"jobStatusInt", int(job.Status),
		"validStatuses", []string{"created", "triggered", "started", "queued_for_run", "succeeded", "failed"})
	return "", fmt.Errorf("unknown job status: %v", job.Status)
}

func GetCheckRunConclusionForJob(job *DiggerJob) (string, error) {
	if job.DependencyStopPending() {
		return "", nil
	}
	if job.DependencyRetiredAt != nil {
		return "skipped", nil
	}
	switch job.Status {
	case orchestrator_scheduler.DiggerJobStarted:
		return "", nil
	case orchestrator_scheduler.DiggerJobTriggered:
		return "", nil
	case orchestrator_scheduler.DiggerJobCreated:
		return "", nil
	case orchestrator_scheduler.DiggerJobQueuedForRun:
		return "", nil
	case orchestrator_scheduler.DiggerJobSucceeded:
		return "success", nil
	case orchestrator_scheduler.DiggerJobFailed:
		return "failure", nil
	}
	slog.Error("Unknown job status in GetCheckRunConclusionForJob - this will cause GitHub API 422 error",
		"jobId", job.DiggerJobID,
		"jobStatus", job.Status,
		"jobStatusInt", int(job.Status),
		"validStatuses", []string{"created", "triggered", "started", "queued_for_run", "succeeded", "failed"})
	return "", fmt.Errorf("unknown job status: %v", job.Status)
}

// DependencySkipReason is display text, not a replacement for provenance proof.
func (j *DiggerJob) DependencySkipReason() string {
	if j.DependencyRetirementOperationID != nil {
		if j.DependencyStopPending() {
			return "future dispatch stopped; prior execution unknown; settlement pending"
		}
		return "future dispatch stopped; prior execution unknown; settled under merged-PR/stopped-Actions policy"
	}
	if j.DependencyRetiredAt == nil {
		return ""
	}
	return fmt.Sprintf("prerequisite `%s` failed; never dispatched", j.DependencyFailureJobID)
}
