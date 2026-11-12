package models

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestLegacyStopPostgresPublicationSerializesFenceAdmission(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, _, child, request := legacyStopFixture(t, db, false)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, request)
	require.NoError(t, err)
	request.Mode, request.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	publisher, holder := commandOriginPostgresActor(t, db)
	fencer, pid := commandOriginPostgresActor(t, db)
	entered := make(chan struct{})
	release := make(chan struct{})
	published := make(chan error, 1)
	go func() {
		published <- publisher.WithPublicationBarrier(batch.ID, func(tx *Database) error { close(entered); <-release; return nil })
	}()
	<-entered
	fenced := make(chan error, 1)
	go func() {
		_, err := fencer.FenceDependencyStop(org.ID, repo.ID, batch.ID, request, "operator")
		fenced <- err
	}()
	commandOriginPostgresWaitForBlocker(t, db, pid, holder)
	require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt, "admission cannot commit while publication owns the barrier")
	close(release)
	require.NoError(t, <-published)
	select {
	case err := <-fenced:
		require.NoError(t, err)
	case <-time.After(8 * time.Second):
		t.Fatal("fence remained blocked after publication")
	}
	require.NoError(t, publisher.WithPublicationBarrier(batch.ID, func(tx *Database) error {
		publication, err := tx.HistoricalPublication(batch.ID)
		require.NoError(t, err)
		require.NotNil(t, publication)
		require.Equal(t, int64(6746869800), *publication.Batch.CommentId)
		return nil
	}))
}
