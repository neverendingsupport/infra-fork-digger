package models

import (
	"cmp"
	"slices"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
)

type DependencyStopOutcomePrerequisite struct {
	BatchCommandOriginJob
	WorkflowRunID      int64 `json:"workflow_run_id"`
	WorkflowRunAttempt int64 `json:"workflow_run_attempt"`
}

type DependencyStopOutcomeBinding struct {
	DiggerRepositoryID uint                              `json:"digger_repository_id"`
	RepositoryID       int64                             `json:"repository_id"`
	RepoFullName       string                            `json:"repo_full_name"`
	PRNumber           int                               `json:"pr_number"`
	SourceCommentID    int64                             `json:"source_comment_id"`
	BodySHA256         string                            `json:"body_sha256"`
	CommitSHA          string                            `json:"commit_sha"`
	Command            scheduler.DiggerCommand           `json:"command"`
	BatchID            string                            `json:"batch_id"`
	AggregateCheckID   string                            `json:"aggregate_check_id"`
	ReportCommentID    int64                             `json:"report_comment_id"`
	Target             BatchCommandOriginJob             `json:"target"`
	Prerequisite       DependencyStopOutcomePrerequisite `json:"original_prerequisite"`
	Manifest           []BatchCommandOriginJob           `json:"manifest"`
}

type DependencyStopOutcomeSettlement struct {
	CheckpointID           string                    `json:"checkpoint_id"`
	InputSHA256            string                    `json:"input_sha256"`
	LocalStateSHA256       string                    `json:"local_state_sha256"`
	OriginalFailure        DependencyStopRunProof    `json:"original_failure"`
	CurrentRoot            DependencyStopCurrentRoot `json:"current_root"`
	Jobs                   []CommandProvenanceJob    `json:"jobs"`
	PRNumber               int                       `json:"pr_number"`
	State                  string                    `json:"state"`
	Merged                 bool                      `json:"merged"`
	PRHeadSHA              string                    `json:"pr_head_sha"`
	MergeCommitSHA         string                    `json:"merge_commit_sha"`
	ProviderCheckedFrom    time.Time                 `json:"provider_checked_from"`
	ProviderCheckedThrough time.Time                 `json:"provider_checked_through"`
	ActionsComplete        bool                      `json:"actions_complete"`
	ActiveActions          map[string]int            `json:"active_actions"`
}

func dependencyStopOutcome(stop DependencyStop, job DiggerJob) *ExecutionOutcome {
	var target, parent *BatchCommandOriginJob
	for i := range stop.Snapshot.Manifest {
		entry := &stop.Snapshot.Manifest[i]
		if entry.DBID == job.ID && entry.JobID == job.DiggerJobID {
			target = entry
		}
		if entry.DBID == stop.Request.FailedPrerequisite.DBID && entry.JobID == stop.Request.FailedPrerequisite.JobID {
			parent = entry
		}
	}
	if target == nil || parent == nil || !dependencyStopMatchesJob(stop, job) {
		return nil
	}
	manifest := slices.Clone(stop.Snapshot.Manifest)
	slices.SortFunc(manifest, func(a, b BatchCommandOriginJob) int { return cmp.Compare(a.DBID, b.DBID) })
	origin := stop.Snapshot.Origin
	bound := &DependencyStopOutcomeBinding{DiggerRepositoryID: stop.RepoID, RepositoryID: origin.RepositoryID, RepoFullName: origin.RepoFullName, PRNumber: origin.PrNumber, SourceCommentID: origin.SourceCommentID, BodySHA256: origin.BodySHA256, CommitSHA: origin.CommitSHA, Command: origin.Command, BatchID: stop.BatchID.String(), AggregateCheckID: stop.Snapshot.AggregateCheckID, ReportCommentID: stop.Snapshot.ReportCommentID, Target: *target, Prerequisite: DependencyStopOutcomePrerequisite{BatchCommandOriginJob: *parent, WorkflowRunID: stop.Request.FailedPrerequisite.WorkflowRunID, WorkflowRunAttempt: stop.Request.FailedPrerequisite.WorkflowRunAttempt}, Manifest: manifest}
	out := &ExecutionOutcome{Version: 1, ContractRevision: 3, Kind: stop.Kind, Reason: "failed_dependency", PrerequisiteJobID: job.DependencyFailureJobID, RetiredAt: stop.FencedAt.UTC(), DispatchFenced: true, PriorExecution: stop.PriorExecution, RecoveryID: stop.OperationID.String(), FenceID: stop.OperationID.String(), PlanSHA256: stop.PlanSHA256, Binding: bound, SettlementPolicy: stop.Request.SettlementPolicy, SettlementState: stop.Phase(), VerifiedAt: nil}
	if stop.CurrentCheckpoint != nil {
		cp := stop.CurrentCheckpoint
		v := cp.Verification
		out.VerifiedAt = dependencyStopUTC(&cp.VerifiedAt)
		settlement := &DependencyStopOutcomeSettlement{CheckpointID: cp.ID.String(), InputSHA256: cp.InputSHA256, LocalStateSHA256: cp.LocalStateSHA256, OriginalFailure: DependencyStopRunProof{v.ParentRunID, v.ParentAttempt, v.ParentStatus, v.ParentConclusion}, PRNumber: v.PRNumber, State: v.PRState, Merged: v.Merged, PRHeadSHA: v.PRHeadSHA, MergeCommitSHA: v.MergeCommitSHA, ProviderCheckedFrom: v.StartedAt.UTC(), ProviderCheckedThrough: v.FinishedAt.UTC(), ActionsComplete: v.ActionsComplete, ActiveActions: v.ActiveCounts}
		for _, state := range cp.JobStates {
			evidence := CommandProvenanceJob{DBID: state.DBID, JobID: state.JobID, Project: state.Project, ProjectCheckID: state.ProjectCheckID, Status: state.Status, WorkflowURL: state.WorkflowURL, WorkflowRunID: state.RunID, WorkflowRunAttempt: state.RunAttempt}
			settlement.Jobs = append(settlement.Jobs, evidence)
			if state.DBID == stop.Request.FailedPrerequisite.DBID {
				settlement.CurrentRoot = DependencyStopCurrentRoot{CommandProvenanceJob: evidence, WorkflowStatus: v.CurrentParentProviderStatus, WorkflowConclusion: v.CurrentParentConclusion}
			}
		}
		out.Settlement = settlement
	}

	return out
}

type DependencyStopRunProof struct {
	WorkflowRunID      int64  `json:"workflow_run_id"`
	WorkflowRunAttempt int64  `json:"workflow_run_attempt"`
	WorkflowStatus     string `json:"workflow_status"`
	WorkflowConclusion string `json:"workflow_conclusion"`
}
type DependencyStopCurrentRoot struct {
	CommandProvenanceJob
	WorkflowStatus     string `json:"workflow_status"`
	WorkflowConclusion string `json:"workflow_conclusion"`
}
