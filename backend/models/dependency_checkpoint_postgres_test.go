package models

import (
	"encoding/json"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"slices"
	"testing"
	"time"
)

func TestLegacyStopPostgresCheckpointRerunExit(t *testing.T) {
	for _, before := range []bool{true, false} {
		for _, success := range []bool{true, false} {
			name := "before_first_verify"
			if !before {
				name = "after_verified"
			}
			if success {
				name += "_success"
			} else {
				name += "_failed"
			}
			t.Run(name, func(t *testing.T) {
				db := commandOriginPostgresDB(t, true)
				batch, org, repo, parent, child, req := legacyStopFixture(t, db, false)
				plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
				require.NoError(t, err)
				req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
				stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
				require.NoError(t, err)
				var firstID string
				var originalVerification []byte
				verify := func(parent DiggerJob) *DependencyStop {
					view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
					require.NoError(t, err)
					evidence := legacyStopEvidence(*stop)
					evidence.CurrentParentRunID = parent.WorkflowRunID
					evidence.CurrentParentAttempt = parent.WorkflowRunAttempt
					evidence.CurrentParentStatus = parent.Status
					if parent.Status == scheduler.DiggerJobSucceeded {
						evidence.CurrentParentConclusion = "success"
					}
					saved, err := db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", evidence)
					require.NoError(t, err)
					return saved
				}
				if !before {
					stop = verify(parent)
					firstID = stop.CurrentCheckpoint.ID.String()
					originalVerification, err = json.Marshal(stop.Verification)
					require.NoError(t, err)
				}
				stale := dependencySavedJob(t, db, parent.ID)
				parent = stale
				parent.Status = scheduler.DiggerJobStarted
				parent.WorkflowRunAttempt = 2
				parent.StatusUpdatedAt = time.Now().UTC()
				next, err := db.AcceptDiggerJobCallback(&parent, org.ID)
				require.NoError(t, err)
				parent = *next
				// The SQL invalidation and terminal guard work before ordinary reconcile.
				var raw DiggerBatch
				require.NoError(t, db.GormDB.First(&raw, "id=?", batch.ID).Error)
				require.Equal(t, scheduler.BatchJobStarted, raw.Status)
				require.Error(t, db.GormDB.Model(&DiggerBatch{}).Where("id=?", batch.ID).Update("status", scheduler.BatchJobFailed).Error)
				saved, err := db.GetDiggerJob(child.DiggerJobID)
				require.NoError(t, err)
				require.True(t, saved.DependencyStopPending())
				parent.Status = scheduler.DiggerJobFailed
				if success {
					parent.Status = scheduler.DiggerJobSucceeded
				}
				parent.StatusUpdatedAt = time.Now().UTC().Add(time.Millisecond)
				next, err = db.AcceptDiggerJobCallback(&parent, org.ID)
				require.NoError(t, err)
				parent = *next
				require.Error(t, db.GormDB.Save(&stale).Error, "an old whole-row writer cannot resurrect a stale checkpoint")
				stop = verify(parent)
				require.Equal(t, "verified", stop.Phase())
				require.Equal(t, int64(1), stop.Request.FailedPrerequisite.WorkflowRunAttempt)
				require.Equal(t, "unknown", stop.PriorExecution)
				require.True(t, stop.FencedAt.Equal(*saved.DependencyRetiredAt))
				currentID := stop.CurrentCheckpoint.ID
				require.NotEqual(t, firstID, currentID.String())
				repeated := verify(parent)
				require.Equal(t, currentID, repeated.CurrentCheckpoint.ID)
				require.True(t, repeated.CurrentCheckpoint.VerifiedAt.Equal(stop.CurrentCheckpoint.VerifiedAt))
				if !before {
					data, err := json.Marshal(repeated.Verification)
					require.NoError(t, err)
					require.JSONEq(t, string(originalVerification), string(data), "first audit evidence is never overwritten")
				}
				proof, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, req.SourceCommentID)
				require.NoError(t, err)
				require.Equal(t, scheduler.BatchJobFailed, proof.Batches[0].Status)
				for _, job := range proof.Batches[0].Jobs {
					if job.JobID == child.DiggerJobID {
						out := job.ExecutionOutcome
						require.NotNil(t, out)
						require.Equal(t, 3, out.ContractRevision)
						require.Equal(t, int64(1), out.Binding.Prerequisite.WorkflowRunAttempt)
						require.Equal(t, int64(2), out.Settlement.CurrentRoot.WorkflowRunAttempt)
						require.Equal(t, parent.Status, out.Settlement.CurrentRoot.Status)
						require.Equal(t, "failure", out.Settlement.OriginalFailure.WorkflowConclusion)
						if success {
							require.Equal(t, "success", out.Settlement.CurrentRoot.WorkflowConclusion)
						}
					}
				}
				var count int64
				require.NoError(t, db.GormDB.Model(&DependencyStopCheckpoint{}).Count(&count).Error)
				expected := int64(1)
				if !before {
					expected = 2
				}
				require.Equal(t, expected, count)
				require.Error(t, db.GormDB.Model(&DependencyStopCheckpoint{}).Where("id=?", currentID).Update("verified_by", "replacement").Error)
				require.Error(t, db.GormDB.Where("id=?", currentID).Delete(&DependencyStopCheckpoint{}).Error)
				claimed, err := db.ClaimDiggerJobDispatch(&child)
				require.NoError(t, err)
				require.False(t, claimed)
			})
		}
	}
}

func TestLegacyStopPostgresProvenanceCheckpointSnapshot(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, parent, child, req := legacyStopFixture(t, db, false)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	first, err := db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.NoError(t, err)
	actor, _ := commandOriginPostgresActor(t, db)
	advanced := false
	require.NoError(t, db.GormDB.Callback().Query().After("gorm:query").Register("test:advance_checkpoint", func(tx *gorm.DB) {
		if advanced || tx.Statement.Table != "digger_jobs" || !slices.Contains(tx.Statement.Selects, "callback_revision") {
			return
		}
		advanced = true
		next, err := actor.GetDiggerJob(parent.DiggerJobID)
		require.NoError(t, err)
		next.Status = scheduler.DiggerJobStarted
		next.WorkflowRunAttempt = 2
		next.StatusUpdatedAt = time.Now().UTC()
		next, err = actor.AcceptDiggerJobCallback(next, org.ID)
		require.NoError(t, err)
		next.Status = scheduler.DiggerJobFailed
		next.StatusUpdatedAt = time.Now().UTC().Add(time.Millisecond)
		_, err = actor.AcceptDiggerJobCallback(next, org.ID)
		require.NoError(t, err)
		current, err := actor.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
		require.NoError(t, err)
		evidence := legacyStopEvidence(*stop)
		evidence.CurrentParentAttempt = 2
		_, err = actor.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, current.StateSHA256, "operator", evidence)
		require.NoError(t, err)
	}))
	t.Cleanup(func() { _ = db.GormDB.Callback().Query().Remove("test:advance_checkpoint") })
	proof, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, req.SourceCommentID)
	require.NoError(t, err)
	require.True(t, advanced)
	var outcome *ExecutionOutcome
	var outerAttempt int64
	for _, job := range proof.Batches[0].Jobs {
		if job.JobID == parent.DiggerJobID {
			outerAttempt = job.WorkflowRunAttempt
		}
		if job.JobID == child.DiggerJobID {
			outcome = job.ExecutionOutcome
		}
	}
	require.NotNil(t, outcome)
	require.NotNil(t, outcome.Settlement)
	require.Equal(t, outerAttempt, outcome.Settlement.CurrentRoot.WorkflowRunAttempt)
	require.Equal(t, int64(1), outerAttempt, "one provenance transaction returns one coherent snapshot")
	require.Equal(t, first.CurrentCheckpoint.ID.String(), outcome.Settlement.CheckpointID)
	later, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, req.SourceCommentID)
	require.NoError(t, err)
	for _, job := range later.Batches[0].Jobs {
		if job.JobID == child.DiggerJobID {
			require.Equal(t, int64(2), job.ExecutionOutcome.Settlement.CurrentRoot.WorkflowRunAttempt)
		}
	}
}
