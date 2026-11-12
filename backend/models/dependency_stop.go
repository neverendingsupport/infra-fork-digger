package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const DependencyStopPolicy = "merged_pr_all_actions_stopped_v1"

var ErrDependencyStopConflict = errors.New("historical dependency stop conflicts with its source, plan or execution state")
var ErrDependencyStopPending = errors.New("historical dependency stop awaits verification")

type DependencyStopJobRef struct {
	DBID  uint   `json:"db_id"`
	JobID string `json:"job_id"`
}

type DependencyStopPrerequisite struct {
	DependencyStopJobRef
	WorkflowRunID      int64 `json:"workflow_run_id"`
	WorkflowRunAttempt int64 `json:"workflow_run_attempt"`
}

type DependencyStopRequest struct {
	Version            int                        `json:"version"`
	Mode               string                     `json:"mode,omitempty"`
	OperationID        uuid.UUID                  `json:"operation_id"`
	PRNumber           int                        `json:"pr_number"`
	SourceCommentID    int64                      `json:"source_comment_id"`
	BodySHA256         string                     `json:"body_sha256"`
	CommitSHA          string                     `json:"commit_sha"`
	Targets            []DependencyStopJobRef     `json:"targets"`
	FailedPrerequisite DependencyStopPrerequisite `json:"failed_prerequisite"`
	SettlementPolicy   string                     `json:"settlement_policy"`
	ExpectedPlanSHA256 string                     `json:"expected_plan_sha256,omitempty"`
}

func (r DependencyStopRequest) Validate() error {
	if r.Version != 1 || r.OperationID == uuid.Nil || r.PRNumber <= 0 || r.SourceCommentID <= 0 || len(r.Targets) != 1 || r.Targets[0].DBID == 0 || r.Targets[0].JobID == "" || !dependencyStopCommitSHA(r.CommitSHA) || r.SettlementPolicy != DependencyStopPolicy || r.FailedPrerequisite.DBID == 0 || r.FailedPrerequisite.JobID == "" || r.FailedPrerequisite.WorkflowRunID <= 0 || r.FailedPrerequisite.WorkflowRunAttempt <= 0 {
		return ErrDependencyStopConflict
	}
	if _, err := hex.DecodeString(r.BodySHA256); err != nil || len(r.BodySHA256) != 64 {
		return ErrDependencyStopConflict
	}
	if r.Mode != "plan" && r.Mode != "execute" && r.Mode != "verify" {
		return ErrDependencyStopConflict
	}
	if r.Mode != "plan" {
		if _, err := hex.DecodeString(r.ExpectedPlanSHA256); err != nil || len(r.ExpectedPlanSHA256) != 64 {
			return ErrDependencyStopConflict
		}
	}
	return nil
}

func (r DependencyStopRequest) canonical() DependencyStopRequest {
	r.Mode, r.ExpectedPlanSHA256 = "", ""
	return r
}

func dependencyStopHash(value interface{}) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func dependencyStopPlanHash(request DependencyStopRequest, snapshot DependencyStopSnapshot) (string, error) {
	return dependencyStopHash(struct {
		Request  DependencyStopRequest  `json:"request"`
		Snapshot DependencyStopSnapshot `json:"snapshot"`
	}{request.canonical(), snapshot})
}

type DependencyStopJobState struct {
	Project        string `json:"project"`
	ProjectCheckID string `json:"project_check_id"`
	Deleted        bool   `json:"deleted"`
	DependencyStopJobRef
	Status           scheduler.DiggerJobStatus `json:"status"`
	CallbackRevision int64                     `json:"callback_revision"`
	StatusUpdatedAt  time.Time                 `json:"status_updated_at"`
	DispatchClaim    *time.Time                `json:"dispatch_claim"`
	RunID            int64                     `json:"workflow_run_id"`
	RunAttempt       int64                     `json:"workflow_run_attempt"`
	WorkflowURL      string                    `json:"workflow_url"`
	Protocol         int                       `json:"dispatch_protocol_version"`
	SpecSHA256       string                    `json:"spec_sha256"`
	RetiredAt        *time.Time                `json:"retired_at"`
	StopID           *uuid.UUID                `json:"stop_id"`
}

type DependencyStopEdge struct {
	ID       uint   `json:"id"`
	JobID    string `json:"job_id"`
	ParentID string `json:"parent_id"`
}

type DependencyStopLink struct {
	ID            uint   `json:"id"`
	JobID         string `json:"job_id"`
	Repo          string `json:"repo"`
	WorkflowRunID int64  `json:"workflow_run_id"`
	GithubJobID   int64  `json:"github_job_id"`
	Deleted       bool   `json:"deleted"`
}

type DependencyStopSnapshot struct {
	BatchID          uuid.UUID                   `json:"batch_id"`
	RepoID           uint                        `json:"repo_id"`
	OrganisationID   uint                        `json:"organisation_id"`
	InstallationID   int64                       `json:"installation_id"`
	Origin           CommandProvenanceOrigin     `json:"origin"`
	AggregateCheckID string                      `json:"aggregate_check_id"`
	ReportCommentID  int64                       `json:"report_comment_id"`
	Manifest         []BatchCommandOriginJob     `json:"manifest"`
	BatchStatus      scheduler.DiggerBatchStatus `json:"batch_status"`
	Jobs             []DependencyStopJobState    `json:"jobs"`
	Edges            []DependencyStopEdge        `json:"edges"`
	Links            []DependencyStopLink        `json:"links"`
}

type DependencyStopPlan struct {
	Version        int                    `json:"version"`
	OperationID    uuid.UUID              `json:"operation_id"`
	PlanSHA256     string                 `json:"plan_sha256"`
	Snapshot       DependencyStopSnapshot `json:"snapshot"`
	ProposedReport string                 `json:"proposed_report"`
}

// DependencyStop records a future admission stop, never an assertion about
// whether the legacy job executed before it was fenced.
type DependencyStop struct {
	CurrentCheckpoint *DependencyStopCheckpoint   `gorm:"-" json:"current_checkpoint,omitempty"`
	OperationID       uuid.UUID                   `gorm:"type:uuid;primaryKey" json:"operation_id"`
	Kind              string                      `gorm:"not null;default:'future_dispatch_fenced'" json:"kind"`
	PriorExecution    string                      `gorm:"not null;default:'unknown'" json:"prior_execution"`
	OrganisationID    uint                        `gorm:"not null" json:"-"`
	RepoID            uint                        `gorm:"not null" json:"-"`
	BatchID           uuid.UUID                   `gorm:"type:text;not null" json:"batch_id"`
	TargetJobDBID     uint                        `gorm:"uniqueIndex;not null" json:"target_db_id"`
	RequestSHA256     string                      `gorm:"size:64;not null" json:"-"`
	PlanSHA256        string                      `gorm:"size:64;not null" json:"plan_sha256"`
	Request           DependencyStopRequest       `gorm:"serializer:json;type:jsonb;not null" json:"-"`
	Snapshot          DependencyStopSnapshot      `gorm:"serializer:json;type:jsonb;not null" json:"-"`
	RequestedBy       string                      `gorm:"not null" json:"requested_by"`
	FencedAt          time.Time                   `gorm:"not null" json:"fenced_at"`
	VerifiedAt        *time.Time                  `json:"first_verified_at,omitempty"`
	VerifiedBy        string                      `gorm:"not null;default:''" json:"verified_by,omitempty"`
	Verification      *DependencyStopVerification `gorm:"serializer:json;type:jsonb" json:"-"`
}

func (DependencyStop) TableName() string { return "digger_dependency_stops" }

func (s DependencyStop) Phase() string {
	if s.CurrentCheckpoint == nil {
		return "fenced_pending_verification"
	}
	return "verified"
}

type DependencyStopVerification struct {
	CurrentParentRunID          int64                     `json:"current_parent_run_id"`
	CurrentParentAttempt        int64                     `json:"current_parent_attempt"`
	CurrentParentStatus         scheduler.DiggerJobStatus `json:"current_parent_status"`
	CurrentParentProviderStatus string                    `json:"current_parent_provider_status"`
	CurrentParentConclusion     string                    `json:"current_parent_conclusion"`
	Version                     int                       `json:"version"`
	RepositoryID                int64                     `json:"repository_id"`
	RepoFullName                string                    `json:"repo_full_name"`
	PRNumber                    int                       `json:"pr_number"`
	Merged                      bool                      `json:"merged"`
	PRState                     string                    `json:"pr_state"`
	PRHeadSHA                   string                    `json:"pr_head_sha"`
	MergeCommitSHA              string                    `json:"merge_commit_sha"`
	SourceCommentID             int64                     `json:"source_comment_id"`
	BodySHA256                  string                    `json:"body_sha256"`
	ParentRunID                 int64                     `json:"parent_run_id"`
	ParentAttempt               int64                     `json:"parent_attempt"`
	ParentStatus                string                    `json:"parent_status"`
	ParentConclusion            string                    `json:"parent_conclusion"`
	ActionsComplete             bool                      `json:"actions_complete"`
	ActiveCounts                map[string]int            `json:"active_counts"`
	StartedAt                   time.Time                 `json:"started_at"`
	FinishedAt                  time.Time                 `json:"finished_at"`
}

var DependencyStopIncompleteStatuses = []string{"queued", "in_progress", "requested", "waiting", "pending"}

func (v DependencyStopVerification) ValidFor(s DependencyStop) bool {
	if v.CurrentParentRunID <= 0 || v.CurrentParentAttempt <= 0 || (v.CurrentParentStatus != scheduler.DiggerJobFailed && v.CurrentParentStatus != scheduler.DiggerJobSucceeded) || v.CurrentParentProviderStatus != "completed" || v.CurrentParentConclusion == "" || len(v.CurrentParentConclusion) > 100 {
		return false
	}
	if v.Version != 1 || v.RepositoryID != s.Snapshot.Origin.RepositoryID || v.RepoFullName != s.Snapshot.Origin.RepoFullName || v.PRNumber != s.Request.PRNumber || !v.Merged || v.PRState != "closed" || !dependencyStopCommitSHA(v.PRHeadSHA) || !dependencyStopCommitSHA(v.MergeCommitSHA) || v.SourceCommentID != s.Request.SourceCommentID || v.BodySHA256 != s.Request.BodySHA256 || v.ParentRunID != s.Request.FailedPrerequisite.WorkflowRunID || v.ParentAttempt != s.Request.FailedPrerequisite.WorkflowRunAttempt || v.ParentStatus != "completed" || v.ParentConclusion != "failure" || !v.ActionsComplete || v.StartedAt.Before(s.FencedAt) || v.FinishedAt.Before(v.StartedAt) || len(v.ActiveCounts) != len(DependencyStopIncompleteStatuses) {
		return false
	}
	for _, status := range DependencyStopIncompleteStatuses {
		if count, ok := v.ActiveCounts[status]; !ok || count != 0 {
			return false
		}
	}
	return true
}

func dependencyStopCommitSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func (j *DiggerJob) DependencyStopPending() bool {
	return j.DependencyRetirementOperationID != nil && (j.DependencyStop == nil || j.DependencyStop.CurrentCheckpoint == nil)
}

func dependencyStopMatchesJob(s DependencyStop, j DiggerJob) bool {
	return dependencyStopAuditValid(s) && j.DependencyRetirementOperationID != nil && *j.DependencyRetirementOperationID == s.OperationID && s.OperationID != uuid.Nil && s.TargetJobDBID == j.ID && j.BatchID != nil && s.BatchID.String() == *j.BatchID && len(s.Request.Targets) == 1 && s.Request.Targets[0].DBID == j.ID && s.Request.Targets[0].JobID == j.DiggerJobID && s.Request.FailedPrerequisite.JobID == j.DependencyFailureJobID && j.DependencyRetiredAt != nil && s.FencedAt.Equal(*j.DependencyRetiredAt) && j.DispatchProtocolVersion == 0 && j.Status == scheduler.DiggerJobFailed && !jobHasExecutionEvidence(j) && (s.VerifiedAt == nil || (s.Verification != nil && s.Verification.ValidFor(s) && !s.VerifiedAt.Before(s.Verification.FinishedAt)))
}

func dependencyStopAuditValid(s DependencyStop) bool {
	r := s.Request
	r.Mode = "plan"
	if r.Validate() != nil || s.Kind != "future_dispatch_fenced" || s.PriorExecution != "unknown" || s.Request.Mode != "" || s.Request.ExpectedPlanSHA256 != "" || s.Request.OperationID != s.OperationID || s.Snapshot.BatchID != s.BatchID || s.Snapshot.RepoID != s.RepoID || s.Snapshot.OrganisationID != s.OrganisationID || s.RequestedBy == "" || s.FencedAt.IsZero() || s.Snapshot.ReportCommentID <= 0 {
		return false
	}
	req, err := dependencyStopHash(s.Request)
	if err != nil || req != s.RequestSHA256 {
		return false
	}
	plan, err := dependencyStopPlanHash(s.Request, s.Snapshot)
	return err == nil && plan == s.PlanSHA256
}

func loadDependencyStops(tx *gorm.DB, jobs []DiggerJob) error {
	ids := []uuid.UUID{}
	for _, job := range jobs {
		if job.DependencyRetirementOperationID != nil {
			ids = append(ids, *job.DependencyRetirementOperationID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	var stops []DependencyStop
	if err := tx.Where("operation_id IN ?", ids).Find(&stops).Error; err != nil {
		return err
	}
	byID := map[uuid.UUID]*DependencyStop{}

	for i := range stops {
		complete := len(jobs) == len(stops[i].Snapshot.Manifest)
		for _, job := range jobs {
			if job.BatchID == nil || *job.BatchID != stops[i].BatchID.String() {
				complete = false
			}
		}
		var err error
		if complete {
			err = loadCurrentDependencyCheckpoint(tx, &stops[i], jobs)
		} else {
			err = (&Database{GormDB: tx}).refreshDependencyStopCheckpoint(&stops[i])
		}
		if err != nil {
			return err
		}
	}

	for i := range stops {
		byID[stops[i].OperationID] = &stops[i]
	}
	for i := range jobs {
		if jobs[i].DependencyRetirementOperationID == nil {
			continue
		}
		stop := byID[*jobs[i].DependencyRetirementOperationID]
		if stop == nil || !dependencyStopMatchesJob(*stop, jobs[i]) {
			return ErrDependencyStopConflict
		}
		jobs[i].DependencyStop = stop
	}
	return nil
}

func dependencyStopStates(jobs []DiggerJob) []DependencyStopJobState {
	states := make([]DependencyStopJobState, 0, len(jobs))
	for _, job := range jobs {
		digest := sha256.Sum256(job.SerializedJobSpec)
		states = append(states, DependencyStopJobState{Project: job.ProjectName, ProjectCheckID: commandOriginString(job.CheckRunId), Deleted: job.DeletedAt.Valid, DependencyStopJobRef: DependencyStopJobRef{job.ID, job.DiggerJobID}, Status: job.Status, CallbackRevision: job.CallbackRevision, StatusUpdatedAt: job.StatusUpdatedAt.UTC(), DispatchClaim: dependencyStopUTC(job.WorkflowDispatchStartedAt), RunID: job.WorkflowRunID, RunAttempt: job.WorkflowRunAttempt, WorkflowURL: commandOriginString(job.WorkflowRunUrl), Protocol: job.DispatchProtocolVersion, SpecSHA256: hex.EncodeToString(digest[:]), RetiredAt: dependencyStopUTC(job.DependencyRetiredAt), StopID: job.DependencyRetirementOperationID})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].DBID < states[j].DBID })
	return states
}

func stopConflict(detail string) error {
	return fmt.Errorf("%w: %s", ErrDependencyStopConflict, detail)
}

func dependencyStopUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	value := t.UTC()
	return &value
}

func effectiveHistoricalStatus(batch *DiggerBatch, jobs []DiggerJob) {
	for _, job := range jobs {
		if job.DependencyStopPending() {
			batch.Status = scheduler.BatchJobStarted
			return
		}
	}
}
