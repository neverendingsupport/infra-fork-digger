package models

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR51PublicationLoadsSummaryInLockedTransaction(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, parent, _, req := legacyStopFixture(t, db, true)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	require.NoError(t, db.WithPublicationBarrier(batch.ID, func(tx *Database) error {
		require.NoError(t, tx.GormDB.Model(&DiggerJobSummary{}).Where("id=?", parent.DiggerJobSummaryID).Update("resources_created", 17).Error)
		publication, err := tx.HistoricalPublication(batch.ID)
		require.NoError(t, err)
		for _, job := range publication.Jobs {
			if job.ID == parent.ID {
				require.Equal(t, uint(17), job.DiggerJobSummary.ResourcesCreated, "summary must use the caller's transaction, including its uncommitted writes")
			}
		}
		return nil
	}))
}
