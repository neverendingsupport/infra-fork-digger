package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func dependencyFixture(t *testing.T) (*Database, DiggerBatch, DiggerJob, DiggerJob) {
	t.Helper()
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	db := newCommandOriginDB(t)
	require.NoError(t, db.GormDB.AutoMigrate(&DiggerJobParentLink{}, &GithubDiggerJobLink{}))
	batch := newCommandOriginBatch(t, db, uuid.New())
	batch.BatchType = scheduler.DiggerCommandApply
	require.NoError(t, db.GormDB.Save(&batch).Error)
	makeJob := func(name string) DiggerJob {
		body, err := json.Marshal(scheduler.JobJson{ProjectName: name, Commit: batch.CommitSha, JobType: string(batch.BatchType), Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber})
		require.NoError(t, err)
		job, err := db.CreateDiggerJob(batch.ID, body, "workflow.yml", nil, nil, "lazy", name)
		require.NoError(t, err)
		return *job
	}
	parent, child := makeJob("parent"), makeJob("child")
	parent.Status = scheduler.DiggerJobFailed
	parent.WorkflowRunID, parent.WorkflowRunAttempt = 42665476673, 1
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	return db, batch, parent, child
}

func dependencySavedJob(t *testing.T, db *Database, id uint) DiggerJob {
	t.Helper()
	var job DiggerJob
	require.NoError(t, db.GormDB.First(&job, id).Error)
	return job
}

func TestFailedDependencyUpdateBatchStatusRetiresUntouchedChild(t *testing.T) {
	db, batch, parent, child := dependencyFixture(t)
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobFailed, batch.Status)
	saved := dependencySavedJob(t, db, child.ID)
	require.Equal(t, scheduler.DiggerJobFailed, saved.Status)
	require.NotNil(t, saved.DependencyRetiredAt)
	require.Equal(t, parent.DiggerJobID, saved.DependencyFailureJobID)
	require.Nil(t, saved.WorkflowDispatchStartedAt)
	require.Equal(t, child.SerializedJobSpec, saved.SerializedJobSpec)
	require.Zero(t, saved.WorkflowRunID)
	require.Equal(t, parent.WorkflowRunID, dependencySavedJob(t, db, parent.ID).WorkflowRunID)
	claimed, err := db.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	queued, err := db.QueueDiggerJobIfCreated(&child)
	require.NoError(t, err)
	require.False(t, queued)
	observed, err := db.ObserveDiggerJobCallback(&child)
	require.NoError(t, err)
	require.False(t, observed)
	require.ErrorIs(t, db.UpdateDiggerJob(&child), ErrDiggerJobRetired)
	restarted := &Database{GormDB: db.GormDB.Session(&gorm.Session{NewDB: true})}
	again, err := restarted.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, []string{child.DiggerJobID}, again.RetiredJobs)
	require.Equal(t, saved.DependencyRetiredAt, dependencySavedJob(t, restarted, child.ID).DependencyRetiredAt)
}

func TestFailedDependencyChainPreservesIndependentRunningSibling(t *testing.T) {
	db, batch, parent, child := dependencyFixture(t)
	grandchild, err := db.CreateDiggerJob(batch.ID, child.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "grandchild")
	require.NoError(t, err)
	require.NoError(t, db.CreateDiggerJobParentLink(child.DiggerJobID, grandchild.DiggerJobID))
	sibling, err := db.CreateDiggerJob(batch.ID, child.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "sibling")
	require.NoError(t, err)
	observed, err := db.ObserveDiggerJobCallback(sibling)
	require.NoError(t, err)
	require.True(t, observed)
	sibling.Status = scheduler.DiggerJobStarted
	require.NoError(t, db.UpdateDiggerJob(sibling))
	result, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{child.DiggerJobID, grandchild.DiggerJobID}, result.RetiredJobs)
	require.Equal(t, []string{sibling.DiggerJobID}, result.UnresolvedJobs)
	require.Equal(t, scheduler.BatchJobCreated, result.BatchStatus)
	require.Equal(t, scheduler.DiggerJobStarted, dependencySavedJob(t, db, sibling.ID).Status)
	// A different successful batch for the same PR cannot settle this batch.
	later := batch
	later.ID, later.Status = uuid.New(), scheduler.BatchJobSucceeded
	require.NoError(t, db.GormDB.Create(&later).Error)
	sibling.Status = scheduler.DiggerJobSucceeded
	require.NoError(t, db.UpdateDiggerJob(sibling))
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobFailed, batch.Status)
	require.Equal(t, parent.WorkflowRunID, dependencySavedJob(t, db, parent.ID).WorkflowRunID)
}

func TestFailedDependencyDispatchEvidenceMatrix(t *testing.T) {
	for _, kind := range []string{"queued", "claimed_created", "triggered", "started", "legacy", "run", "url", "link", "created_callback"} {
		t.Run(kind, func(t *testing.T) {
			db, batch, _, child := dependencyFixture(t)
			switch kind {
			case "queued":
				require.NoError(t, db.GormDB.Model(&child).Update("status", scheduler.DiggerJobQueuedForRun).Error)
			case "claimed_created":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET workflow_dispatch_started_at = ? WHERE id = ?", time.Now(), child.ID).Error)
			case "triggered":
				require.NoError(t, db.GormDB.Model(&child).Update("status", scheduler.DiggerJobTriggered).Error)
			case "started":
				require.NoError(t, db.GormDB.Model(&child).Update("status", scheduler.DiggerJobStarted).Error)
			case "legacy":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET dispatch_protocol_version = 0 WHERE id = ?", child.ID).Error)
			case "run":
				require.NoError(t, db.GormDB.Model(&child).Update("workflow_run_id", 88).Error)
			case "url":
				require.NoError(t, db.GormDB.Model(&child).Update("workflow_run_url", "https://github.com/owner/repo/actions/runs/88").Error)
			case "link":
				require.NoError(t, db.GormDB.Create(&GithubDiggerJobLink{DiggerJobId: child.DiggerJobID, GithubJobId: 88}).Error)
			case "created_callback":
				observed, err := db.ObserveDiggerJobCallback(&child)
				require.NoError(t, err)
				require.True(t, observed)
				require.NoError(t, db.UpdateDiggerJob(&child))
			}
			result, err := db.ReconcileFailedDependencies(batch.ID)
			require.NoError(t, err)
			saved := dependencySavedJob(t, db, child.ID)
			if kind == "queued" {
				require.NotNil(t, saved.DependencyRetiredAt)
				require.Equal(t, scheduler.BatchJobFailed, result.BatchStatus)
			} else {
				require.Nil(t, saved.DependencyRetiredAt)
				require.Empty(t, result.RetiredJobs)
				require.Equal(t, []string{child.DiggerJobID}, result.UnresolvedJobs)
				require.Equal(t, scheduler.BatchJobCreated, result.BatchStatus)
			}
		})
	}
}

func TestFailedDependencyMalformedGraphFailsBeforeAnyFence(t *testing.T) {
	for _, kind := range []string{"cross_batch", "missing", "cycle", "deleted", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			db, batch, parent, child := dependencyFixture(t)
			switch kind {
			case "cross_batch":
				other := batch
				other.ID = uuid.New()
				require.NoError(t, db.GormDB.Create(&other).Error)
				require.NoError(t, db.GormDB.Model(&parent).Update("batch_id", other.ID.String()).Error)
			case "missing":
				require.NoError(t, db.CreateDiggerJobParentLink("missing", child.DiggerJobID))
			case "cycle":
				require.NoError(t, db.CreateDiggerJobParentLink(child.DiggerJobID, parent.DiggerJobID))
			case "deleted":
				require.NoError(t, db.GormDB.Delete(&parent).Error)
			case "duplicate":
				parent.ID = 0
				require.NoError(t, db.GormDB.Create(&parent).Error)
			}
			_, err := db.ReconcileFailedDependencies(batch.ID)
			require.Error(t, err)
			require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
		})
	}
}

func TestFailedDependencyProvenanceIsAdditiveAndSourceBound(t *testing.T) {
	db := newCommandOriginDB(t)
	require.NoError(t, db.GormDB.AutoMigrate(&DiggerJobParentLink{}, &GithubDiggerJobLink{}))
	batch := newCommandOriginBatch(t, db, uuid.New())
	orgID := setCommandOriginScope(t, db, batch.GithubInstallationId)
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"parent", "child"}))
	makeJob := func(project string) DiggerJob {
		j := newCommandOriginJob(t, db, batch, project)
		require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET dispatch_protocol_version = 1, workflow_run_id = 0, workflow_run_attempt = 0, workflow_run_url = '#', status = 1 WHERE id = ?", j.ID).Error)
		return dependencySavedJob(t, db, j.ID)
	}
	parent, child := makeJob("parent"), makeJob("child")
	parent.Status, parent.WorkflowRunID, parent.WorkflowRunAttempt = scheduler.DiggerJobFailed, 42665476673, 1
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	repaired, err := db.ReconcileCommandDependencies(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID, batch.CommitSha, batch.ID)
	require.NoError(t, err)
	require.Equal(t, scheduler.BatchJobFailed, repaired.BatchStatus)
	evidence, err := db.GetCommandProvenance(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
	require.NoError(t, err)
	require.Equal(t, 1, evidence.Version)
	require.True(t, evidence.Batches[0].ManifestComplete)
	encoded, err := json.Marshal(evidence)
	require.NoError(t, err)
	// Frozen v1 reader ignores the additive proof; original numeric statuses survive.
	var previous struct {
		Version int `json:"version"`
		Batches []struct {
			Status int `json:"status"`
			Jobs   []struct {
				JobID  string `json:"job_id"`
				Status int    `json:"status"`
				Run    int64  `json:"workflow_run_id"`
			} `json:"jobs"`
		} `json:"batches"`
	}
	require.NoError(t, json.Unmarshal(encoded, &previous))
	require.Equal(t, 1, previous.Version)
	require.Equal(t, 3, previous.Batches[0].Status)
	require.Len(t, previous.Batches[0].Jobs, 2)
	for _, job := range evidence.Batches[0].Jobs {
		require.Equal(t, scheduler.DiggerJobFailed, job.Status)
		if job.JobID == child.DiggerJobID {
			require.NotNil(t, job.ExecutionOutcome)
			require.Equal(t, "never_dispatched", job.ExecutionOutcome.Kind)
			require.Equal(t, parent.DiggerJobID, job.ExecutionOutcome.PrerequisiteJobID)
			require.True(t, job.ExecutionOutcome.DispatchFenced)
		} else {
			require.Nil(t, job.ExecutionOutcome)
			require.Equal(t, parent.WorkflowRunID, job.WorkflowRunID)
		}
	}
	// Contradictory later provider attribution must withhold the proof, even
	// though the immutable source manifest itself still matches.
	require.NoError(t, db.GormDB.Create(&GithubDiggerJobLink{DiggerJobId: child.DiggerJobID, GithubJobId: 99}).Error)
	contradictory, err := db.GetCommandProvenance(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
	require.NoError(t, err)
	require.True(t, contradictory.Batches[0].ManifestComplete)
	for _, job := range contradictory.Batches[0].Jobs {
		require.Nil(t, job.ExecutionOutcome)
	}
	_, err = db.ReconcileCommandDependencies(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID, "changed-head", batch.ID)
	require.Error(t, err)
	require.NoError(t, db.GormDB.Model(&child).Update("project_name", "changed-source").Error)
	evidence, err = db.GetCommandProvenance(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
	require.NoError(t, err)
	require.False(t, evidence.Batches[0].ManifestComplete)
	for _, job := range evidence.Batches[0].Jobs {
		require.Nil(t, job.ExecutionOutcome)
	}
}

func TestFailedDependencyTraversesClaimedIntermediate(t *testing.T) {
	db, batch, parent, child := dependencyFixture(t)
	observed, err := db.ObserveDiggerJobCallback(&child)
	require.NoError(t, err)
	require.True(t, observed)
	child.Status = scheduler.DiggerJobStarted
	require.NoError(t, db.UpdateDiggerJob(&child))
	grandchild, err := db.CreateDiggerJob(batch.ID, child.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "grandchild")
	require.NoError(t, err)
	require.NoError(t, db.CreateDiggerJobParentLink(child.DiggerJobID, grandchild.DiggerJobID))
	result, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, []string{grandchild.DiggerJobID}, result.RetiredJobs)
	require.Equal(t, []string{child.DiggerJobID}, result.UnresolvedJobs)
	require.Equal(t, scheduler.BatchJobCreated, result.BatchStatus)
	require.Equal(t, parent.DiggerJobID, dependencySavedJob(t, db, grandchild.ID).DependencyFailureJobID)
	require.Equal(t, scheduler.DiggerJobStarted, dependencySavedJob(t, db, child.ID).Status)
	saved := dependencySavedJob(t, db, grandchild.ID)
	serialized, err := saved.MapToJsonStruct()
	require.NoError(t, err)
	require.Contains(t, serialized.SkipReason, "never dispatched")
}

func TestDispatchRequiresPositiveParentStateAtPersistentBoundary(t *testing.T) {
	for _, kind := range []string{"failed", "unknown_status", "missing_batch", "missing_parent", "cross_batch"} {
		t.Run(kind, func(t *testing.T) {
			db, _, parent, child := dependencyFixture(t)
			parent.Status = scheduler.DiggerJobSucceeded
			require.NoError(t, db.UpdateDiggerJob(&parent))
			switch kind {
			case "failed":
				require.NoError(t, db.GormDB.Model(&parent).Update("status", scheduler.DiggerJobFailed).Error)
			case "unknown_status":
				require.NoError(t, db.GormDB.Model(&parent).Update("status", nil).Error)
			case "missing_batch":
				require.NoError(t, db.GormDB.Model(&parent).Update("batch_id", nil).Error)
			case "missing_parent":
				require.NoError(t, db.GormDB.Delete(&parent).Error)
			case "cross_batch":
				require.NoError(t, db.GormDB.Model(&parent).Update("batch_id", uuid.NewString()).Error)
			}
			claimed, err := db.ClaimDiggerJobDispatch(&child)
			require.NoError(t, err)
			require.False(t, claimed, "a stale successful-parent view cannot authorize this send")
			require.Nil(t, dependencySavedJob(t, db, child.ID).WorkflowDispatchStartedAt)
		})
	}
}

func TestFailedDependencyUnqualifiedProducerCannotMintProof(t *testing.T) {
	db, batch, parent, _ := dependencyFixture(t)
	drift, err := db.CreateCiJobFromSpec(spec.Spec{JobId: "separate-drift-job", Job: scheduler.JobJson{ProjectName: "drift"}}, "drift", "drift", batch.ID.String())
	require.NoError(t, err)
	require.Zero(t, drift.DispatchProtocolVersion)
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, drift.DiggerJobID))
	repaired, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, "legacy_dispatch_history", repaired.UnresolvedReasons[drift.DiggerJobID])
	require.Nil(t, dependencySavedJob(t, db, drift.ID).DependencyRetiredAt)
}

func TestFailedDependencyKeepsAggregateOpenForRealParentRerun(t *testing.T) {
	db, batch, parent, child := dependencyFixture(t)
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobFailed, batch.Status)
	retired := dependencySavedJob(t, db, child.ID).DependencyRetiredAt
	observed, err := db.ObserveDiggerJobCallback(&parent)
	require.NoError(t, err)
	require.True(t, observed)
	parent.Status = scheduler.DiggerJobStarted
	parent.WorkflowRunAttempt = 2
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobStarted, batch.Status)
	require.Equal(t, retired, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
	parent.Status = scheduler.DiggerJobSucceeded
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobFailed, batch.Status, "the old descendant cannot become runnable again")
}
