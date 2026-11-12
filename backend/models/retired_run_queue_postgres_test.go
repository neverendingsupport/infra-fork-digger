package models

import (
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestFailedRunQueuePostgresNewerCallbackWins(t *testing.T) {
	for _, initial := range []DiggerRunStatus{RunQueued, RunApproved} {
		t.Run(string(initial), func(t *testing.T) {
			db, batch, parent, _ := dependencyPostgresFixture(t)
			orgID := setCommandOriginScope(t, db, batch.GithubInstallationId)
			repo := Repo{Name: "queue", RepoFullName: batch.RepoFullName, OrganisationID: orgID}
			require.NoError(t, db.GormDB.Create(&repo).Error)
			parent.WorkflowRunID, parent.WorkflowRunAttempt = 42665476673, 1
			parent.StatusUpdatedAt = time.Date(2026, 11, 12, 5, 0, 1, 0, time.UTC)
			require.NoError(t, db.UpdateDiggerJob(&parent))
			require.Nil(t, parent.DependencyRetiredAt, "ordinary failure can be superseded by a rerun")
			batchID := batch.ID.String()
			stage := DiggerRunStage{BatchID: &batchID}
			run := DiggerRun{RepoId: repo.ID, Status: initial, PlanStage: stage, ApplyStage: stage}
			require.NoError(t, db.GormDB.Create(&run).Error)
			project := Project{Name: "queue", OrganisationID: orgID, RepoFullName: batch.RepoFullName}
			require.NoError(t, db.GormDB.Create(&project).Error)
			item := DiggerRunQueueItem{ProjectId: project.ID, DiggerRunId: run.ID, DiggerRun: run}
			require.NoError(t, db.GormDB.Create(&item).Error)

			// The run lock exposes the old read-job/lock-run window. Correct
			// settlement waits at the batch lock and rereads the newer job.
			lock := db.GormDB.Begin()
			require.NoError(t, lock.Error)
			t.Cleanup(func() { _ = lock.Rollback().Error })
			var holder int
			require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
			var heldBatch DiggerBatch
			require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&heldBatch, "id = ?", batch.ID).Error)
			var heldRun DiggerRun
			require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&heldRun, run.ID).Error)
			actor, pid := commandOriginPostgresActor(t, db)
			failures := make(chan error, 1)
			go func() { failures <- actor.SettleFailedRunQueueItem(&item, &parent) }()
			commandOriginPostgresWaitForBlocker(t, db, pid, holder)

			newer := parent
			newer.Status, newer.WorkflowRunAttempt = scheduler.DiggerJobStarted, 2
			newer.StatusUpdatedAt = parent.StatusUpdatedAt.Add(time.Second)
			_, err := (&Database{GormDB: lock}).AcceptDiggerJobCallback(&newer, orgID)
			require.NoError(t, err)
			require.NoError(t, lock.Commit().Error)
			select {
			case err := <-failures:
				require.Error(t, err, "newer running attempt must refuse settlement")
			case <-time.After(8 * time.Second):
				t.Fatal("settlement did not return")
			}
			saved := dependencySavedJob(t, db, parent.ID)
			require.Equal(t, scheduler.DiggerJobStarted, saved.Status)
			require.Equal(t, int64(2), saved.WorkflowRunAttempt)
			var current DiggerRun
			require.NoError(t, db.GormDB.First(&current, run.ID).Error)
			require.Equal(t, initial, current.Status)
			var retained int64
			require.NoError(t, db.GormDB.Model(&DiggerRunQueueItem{}).Where("id = ?", item.ID).Count(&retained).Error)
			require.Equal(t, int64(1), retained)
		})
	}
}
