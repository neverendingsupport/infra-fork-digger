package models

import (
	"errors"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"reflect"
	"slices"
	"time"
)

// DependencyStopCheckpoint is append-only. It never changes the original fence
// request, failed cause, first verification or unknown execution history.
type DependencyStopCheckpoint struct {
	ID               uuid.UUID                  `gorm:"type:uuid;primaryKey" json:"checkpoint_id"`
	OperationID      uuid.UUID                  `gorm:"type:uuid;not null;uniqueIndex:idx_dependency_checkpoint_input" json:"operation_id"`
	InputSHA256      string                     `gorm:"size:64;not null;uniqueIndex:idx_dependency_checkpoint_input" json:"input_sha256"`
	LocalStateSHA256 string                     `gorm:"size:64;not null;index" json:"local_state_sha256"`
	JobStates        []DependencyStopJobState   `gorm:"serializer:json;type:jsonb;not null" json:"-"`
	Verification     DependencyStopVerification `gorm:"serializer:json;type:jsonb;not null" json:"-"`
	VerifiedAt       time.Time                  `gorm:"not null" json:"verified_at"`
	VerifiedBy       string                     `gorm:"not null" json:"verified_by"`
}

func (DependencyStopCheckpoint) TableName() string { return "digger_dependency_stop_checkpoints" }

func loadCurrentDependencyCheckpoint(tx *gorm.DB, stop *DependencyStop, jobs []DiggerJob) error {
	stop.CurrentCheckpoint = nil
	current, err := dependencyStopStaticCurrent(tx, *stop, jobs)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}
	digest, err := dependencyStopHash(dependencyStopStates(jobs))
	if err != nil {
		return err
	}
	var cp DependencyStopCheckpoint
	err = tx.Where("operation_id=? AND local_state_sha256=?", stop.OperationID, digest).Order("verified_at DESC, id DESC").First(&cp).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	actual, err := dependencyStopHash(cp.JobStates)
	if err != nil {
		return err
	}
	if actual != digest || !cp.Verification.ValidFor(*stop) || cp.VerifiedAt.Before(cp.Verification.FinishedAt) {
		return ErrDependencyStopConflict
	}
	stop.CurrentCheckpoint = &cp
	return nil
}

func (db *Database) refreshDependencyStopCheckpoint(stop *DependencyStop) error {
	var jobs []DiggerJob
	if err := db.GormDB.Unscoped().Where("batch_id=?", stop.BatchID.String()).Order("id ASC").Limit(MaxCommandProvenanceJobs + 1).Find(&jobs).Error; err != nil {
		return err
	}
	if len(jobs) > MaxCommandProvenanceJobs {
		return ErrProvenanceTooLarge
	}
	return loadCurrentDependencyCheckpoint(db.GormDB, stop, jobs)
}

func dependencyCheckpointInput(state string, v DependencyStopVerification) (string, error) {
	v.StartedAt = time.Time{}
	v.FinishedAt = time.Time{}
	return dependencyStopHash(struct {
		State    string
		Evidence DependencyStopVerification
	}{state, v})
}

func checkpointParentMatches(v DependencyStopVerification, stop DependencyStop, jobs []DependencyStopJobState) bool {
	for _, j := range jobs {
		if j.DBID == stop.Request.FailedPrerequisite.DBID && j.JobID == stop.Request.FailedPrerequisite.JobID {
			return (j.Status == scheduler.DiggerJobFailed || j.Status == scheduler.DiggerJobSucceeded) && j.Status == v.CurrentParentStatus && j.RunID == v.CurrentParentRunID && j.RunAttempt == v.CurrentParentAttempt && j.RunID > 0 && j.RunAttempt > 0
		}
	}
	return false
}

func dependencyStopStaticCurrent(tx *gorm.DB, stop DependencyStop, jobs []DiggerJob) (bool, error) {
	var batch DiggerBatch
	if err := tx.First(&batch, "id=?", stop.BatchID).Error; err != nil {
		return false, err
	}
	if batch.CommentId == nil || *batch.CommentId != stop.Snapshot.ReportCommentID || batch.CheckRunId == nil || *batch.CheckRunId != stop.Snapshot.AggregateCheckID || batch.RepoOwner+"/"+batch.RepoName != stop.Snapshot.Origin.RepoFullName || batch.GithubInstallationId != stop.Snapshot.InstallationID {
		return false, nil
	}
	var origin BatchCommandOrigin
	if err := tx.First(&origin, "batch_id=?", stop.BatchID).Error; err != nil {
		return false, err
	}
	manifest, err := commandOriginManifest(origin, jobs)
	currentOrigin := CommandProvenanceOrigin{RepositoryID: origin.RepositoryID, RepoFullName: origin.RepoFullName, PrNumber: origin.PrNumber, SourceCommentID: origin.SourceCommentID, BodySHA256: origin.BodySHA256, CommitSHA: origin.CommitSHA, Command: origin.Command}
	if currentOrigin != stop.Snapshot.Origin {
		return false, nil
	}
	if err != nil || !origin.Sealed || !commandOriginMatchesBatch(origin, batch) || !slices.Equal(manifest, stop.Snapshot.Manifest) {
		return false, nil
	}
	ids := []string{}
	for _, j := range jobs {
		ids = append(ids, j.DiggerJobID)
	}
	var links []DiggerJobParentLink
	if err := tx.Where("digger_job_id IN ? OR parent_digger_job_id IN ?", ids, ids).Order("id ASC").Find(&links).Error; err != nil {
		return false, err
	}
	edges := []DependencyStopEdge{}
	for _, l := range links {
		edges = append(edges, DependencyStopEdge{ID: l.ID, JobID: l.DiggerJobId, ParentID: l.ParentDiggerJobId})
	}
	if !reflect.DeepEqual(edges, stop.Snapshot.Edges) {
		return false, nil
	}
	var attribution []GithubDiggerJobLink
	if err := tx.Unscoped().Where("digger_job_id=?", stop.Request.Targets[0].JobID).Order("id ASC").Find(&attribution).Error; err != nil {
		return false, err
	}
	retained := []DependencyStopLink{}
	for _, l := range attribution {
		retained = append(retained, DependencyStopLink{ID: l.ID, JobID: l.DiggerJobId, Repo: l.RepoFullName, WorkflowRunID: l.GithubWorkflowRunId, GithubJobID: l.GithubJobId, Deleted: l.DeletedAt.Valid})
	}
	return reflect.DeepEqual(retained, stop.Snapshot.Links), nil
}
