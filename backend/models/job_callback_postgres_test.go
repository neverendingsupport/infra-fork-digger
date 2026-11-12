package models

import (
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestJobCallbackPostgresRejectsConcurrentStaleFailure(t *testing.T) {
	for _, staleKind := range []string{"older_attempt", "older_timestamp"} {
		t.Run(staleKind, func(t *testing.T) {
			db, batch, parent, child := dependencyPostgresFixture(t)
			t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
			orgID := setCommandOriginScope(t, db, batch.GithubInstallationId)
			require.NoError(t, db.GormDB.Create(&Repo{Name: "callback", RepoFullName: batch.RepoFullName, OrganisationID: orgID}).Error)
			parent.Status = scheduler.DiggerJobCreated
			require.NoError(t, db.UpdateDiggerJob(&parent))
			var previous dependencyPreviousJob
			require.NoError(t, db.GormDB.First(&previous, parent.ID).Error)
			newer := parent
			newer.Status = scheduler.DiggerJobStarted
			newer.WorkflowRunID, newer.WorkflowRunAttempt = 42665476673, 2
			newer.StatusUpdatedAt = time.Date(2026, 11, 12, 5, 0, 2, 0, time.UTC)
			url := "https://github.com/owner/repo/actions/runs/42665476673"
			newer.WorkflowRunUrl = &url
			stale := newer
			stale.Status = scheduler.DiggerJobFailed
			stale.StatusUpdatedAt = newer.StatusUpdatedAt.Add(-time.Second)
			if staleKind == "older_attempt" {
				stale.WorkflowRunAttempt = 1
			}
			lock := db.GormDB.Begin()
			require.NoError(t, lock.Error)
			t.Cleanup(func() { _ = lock.Rollback().Error })
			var holder int
			require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
			var held DiggerBatch
			require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&held, "id = ?", batch.ID).Error)
			actor, pid := commandOriginPostgresActor(t, db)
			errors := make(chan error, 1)
			go func() { _, err := actor.AcceptDiggerJobCallback(&stale, orgID); errors <- err }()
			commandOriginPostgresWaitForBlocker(t, db, pid, holder)
			accepted, err := (&Database{GormDB: lock}).AcceptDiggerJobCallback(&newer, orgID)
			require.NoError(t, err)
			require.Equal(t, int64(1), accepted.CallbackRevision)
			require.NoError(t, lock.Commit().Error)
			select {
			case err := <-errors:
				require.ErrorIs(t, err, ErrDiggerJobTransitionConflict)
			case <-time.After(8 * time.Second):
				t.Fatal("stale callback did not return")
			}
			require.NoError(t, db.UpdateBatchStatus(&batch))
			saved := dependencySavedJob(t, db, parent.ID)
			require.Equal(t, scheduler.DiggerJobStarted, saved.Status)
			require.Equal(t, int64(2), saved.WorkflowRunAttempt)
			require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			require.NotEqual(t, scheduler.BatchJobFailed, batch.Status)
			stale.CallbackRevision = 0
			require.ErrorIs(t, db.UpdateDiggerJob(&stale), ErrDiggerJobTransitionConflict)
			previous.Previous.Status = scheduler.DiggerJobFailed
			require.Error(t, db.GormDB.Save(&previous).Error, "previous whole-row writers cannot bypass callback revision")
			for _, kind := range []string{"legacy", "conflicting_run", "missing_timestamp"} {
				bad := newer
				bad.Status = scheduler.DiggerJobFailed
				bad.StatusUpdatedAt = newer.StatusUpdatedAt.Add(time.Second)
				switch kind {
				case "legacy":
					bad.WorkflowRunID, bad.WorkflowRunAttempt = 0, 0
				case "conflicting_run":
					bad.WorkflowRunID++
				case "missing_timestamp":
					bad.StatusUpdatedAt = time.Time{}
				}
				_, err := db.AcceptDiggerJobCallback(&bad, orgID)
				require.ErrorIs(t, err, ErrDiggerJobTransitionConflict, kind)
			}
			// A failure from the actual current attempt still authorizes retirement.
			current := newer
			current.Status = scheduler.DiggerJobFailed
			current.StatusUpdatedAt = newer.StatusUpdatedAt.Add(time.Second)
			_, err = db.AcceptDiggerJobCallback(&current, orgID)
			require.NoError(t, err)
			require.NoError(t, db.UpdateBatchStatus(&batch))
			require.Equal(t, scheduler.BatchJobFailed, batch.Status)
			require.NotNil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
		})
	}
}
