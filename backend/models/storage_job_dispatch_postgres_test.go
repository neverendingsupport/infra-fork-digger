package models

import (
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func dispatchPostgresJob(t *testing.T, db *Database, project string) DiggerJob {
	t.Helper()
	batch := newCommandOriginBatch(t, db, uuid.New())
	old := commandOriginPostgresJob(t, db, batch, project)
	require.NoError(t, db.GormDB.Model(&DiggerJob{}).Where("id = ?", old.ID).Update("status", scheduler.DiggerJobCreated).Error)
	var job DiggerJob
	require.NoError(t, db.GormDB.First(&job, old.ID).Error)
	return job
}

func TestDispatchPostgresConcurrentClaims(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	job := dispatchPostgresJob(t, db, "shared-child")
	lock := db.GormDB.Begin()
	require.NoError(t, lock.Error)
	t.Cleanup(func() { _ = lock.Rollback().Error })
	var holder int
	require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
	var row DiggerJob
	require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, job.ID).Error)
	type outcome struct {
		claimed bool
		err     error
	}
	results := make(chan outcome, 2)
	for range 2 {
		actor, pid := commandOriginPostgresActor(t, db)
		go func() {
			claimed, err := actor.ClaimDiggerJobDispatch(&job)
			results <- outcome{claimed, err}
		}()
		commandOriginPostgresWaitForBlocker(t, db, pid, holder)
	}
	require.NoError(t, lock.Commit().Error)
	wins := 0
	for range 2 {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			if result.claimed {
				wins++
			}
		case <-time.After(8 * time.Second):
			t.Fatal("dispatch claim did not return")
		}
	}
	require.Equal(t, 1, wins)
	require.NoError(t, db.GormDB.First(&row, job.ID).Error)
	require.NotNil(t, row.WorkflowDispatchStartedAt)
	require.Equal(t, scheduler.DiggerJobTriggered, row.Status)
}

func TestDispatchPostgresStaleWritersPreserveClaim(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	stale := dispatchPostgresJob(t, db, "stale-writer")
	var previous commandOriginPreviousJob
	require.NoError(t, db.GormDB.First(&previous, stale.ID).Error)
	claimed, err := db.ClaimDiggerJobDispatch(&stale)
	require.NoError(t, err)
	require.True(t, claimed)
	// Both the current status endpoint's whole-model Save and the previous
	// release's frozen writer read before the claim, then report created.
	require.NoError(t, db.UpdateDiggerJob(&stale))
	require.NoError(t, db.GormDB.Save(&previous).Error)
	var saved DiggerJob
	require.NoError(t, db.GormDB.First(&saved, stale.ID).Error)
	require.Equal(t, scheduler.DiggerJobCreated, saved.Status)
	require.NotNil(t, saved.WorkflowDispatchStartedAt)
	claimed, err = db.ClaimDiggerJobDispatch(&saved)
	require.NoError(t, err)
	require.False(t, claimed)
	queued, err := db.QueueDiggerJobIfCreated(&saved)
	require.NoError(t, err)
	require.False(t, queued)
	count, err := db.CountActiveDiggerJobDispatches(*saved.BatchID)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	saved.Status = scheduler.DiggerJobSucceeded
	require.NoError(t, db.UpdateDiggerJob(&saved))
	count, err = db.CountActiveDiggerJobDispatches(*saved.BatchID)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestDispatchPostgresRejectsWrongIdentityAndClosedStates(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	job := dispatchPostgresJob(t, db, "restricted")
	wrong := job
	wrong.DiggerJobID = "not-this-job"
	claimed, err := db.ClaimDiggerJobDispatch(&wrong)
	require.NoError(t, err)
	require.False(t, claimed)
	wrong = job
	otherBatch := uuid.NewString()
	wrong.BatchID = &otherBatch
	claimed, err = db.ClaimDiggerJobDispatch(&wrong)
	require.NoError(t, err)
	require.False(t, claimed)
	for _, status := range []scheduler.DiggerJobStatus{scheduler.DiggerJobTriggered, scheduler.DiggerJobStarted, scheduler.DiggerJobFailed, scheduler.DiggerJobSucceeded} {
		require.NoError(t, db.GormDB.Model(&job).Update("status", status).Error)
		claimed, err = db.ClaimDiggerJobDispatch(&job)
		require.NoError(t, err)
		require.False(t, claimed)
	}
	require.NoError(t, db.GormDB.Model(&job).Update("status", scheduler.DiggerJobQueuedForRun).Error)
	claimed, err = db.ClaimDiggerJobDispatch(&job)
	require.NoError(t, err)
	require.True(t, claimed)
	deleted := dispatchPostgresJob(t, db, "deleted")
	require.NoError(t, db.GormDB.Delete(&deleted).Error)
	claimed, err = db.ClaimDiggerJobDispatch(&deleted)
	require.NoError(t, err)
	require.False(t, claimed)
}
