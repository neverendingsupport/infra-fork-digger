package models

import (
	"fmt"
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProducerRepositoryPostgresConcurrentBinding(t *testing.T) {
	for _, vcs := range []DiggerVCSType{DiggerVCSGitlab, DiggerVCSBitbucket} {
		t.Run(string(vcs), func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			org := Organisation{Name: "owner", ExternalId: "owner", ExternalSource: "test"}
			require.NoError(t, db.GormDB.Create(&org).Error)
			// Older schemas/project reports can leave SQL NULL rather than ''.
			require.NoError(t, db.GormDB.Exec("INSERT INTO repos(name, organisation_id, repo_full_name) VALUES (?, ?, NULL)", "group-repo", org.ID).Error)
			first, _ := commandOriginPostgresActor(t, db)
			second, _ := commandOriginPostgresActor(t, db)
			start, done := make(chan struct{}), make(chan error, 2)
			for _, actor := range []*Database{first, second} {
				go func(actor *Database) {
					<-start
					done <- actor.EnsureProducerRepository(org.ID, vcs, "group/repo", "group", "repo")
				}(actor)
			}
			close(start)
			require.NoError(t, <-done)
			require.NoError(t, <-done)
			var owned []Repo
			require.NoError(t, db.GormDB.Where("organisation_id = ? AND repo_full_name = ?", org.ID, "group/repo").Find(&owned).Error)
			require.Len(t, owned, 1)
			require.Equal(t, vcs, owned[0].VCS)
		})
	}
}

func TestOrdinaryLargeBatchPostgresAggregate(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	batchID := batch.ID.String()
	summary := DiggerJobSummary{}
	require.NoError(t, db.GormDB.Create(&summary).Error)
	jobs := make([]DiggerJob, MaxCommandProvenanceJobs+1)
	for i := range jobs {
		jobs[i] = DiggerJob{DiggerJobID: fmt.Sprintf("large-%d", i), BatchID: &batchID, Status: scheduler.DiggerJobSucceeded, DiggerJobSummaryID: summary.ID}
	}
	jobs[len(jobs)-1].Status = scheduler.DiggerJobStarted
	require.NoError(t, db.GormDB.CreateInBatches(&jobs, 50).Error)
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.NotEqual(t, scheduler.BatchJobFailed, batch.Status)
	require.NotEqual(t, scheduler.BatchJobSucceeded, batch.Status)
	require.NoError(t, db.GormDB.Model(&jobs[len(jobs)-1]).Update("status", scheduler.DiggerJobFailed).Error)
	require.NoError(t, db.UpdateBatchStatus(&batch))
	require.Equal(t, scheduler.BatchJobFailed, batch.Status)
	_, err := db.ReconcileFailedDependencies(batch.ID)
	require.ErrorIs(t, err, ErrProvenanceTooLarge)
	var retired int64
	require.NoError(t, db.GormDB.Model(&DiggerJob{}).Where("batch_id = ? AND dependency_retired_at IS NOT NULL", batchID).Count(&retired).Error)
	require.Zero(t, retired)
}
