package models

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Compete with the exact origin INSERT used by .nes.13, on independent
// PostgreSQL sessions. No Go mutex or ordering assumption can settle this race.
func TestCommandRejectionPostgresCompetesWithOldProducer(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	source := commandSource()
	org := setCommandOriginScope(t, db, batch.GithubInstallationId)
	repo := Repo{Name: "test", RepoFullName: batch.RepoFullName, OrganisationID: org, GithubAppId: 23, GithubAppInstallationId: batch.GithubInstallationId}
	require.NoError(t, db.GormDB.Create(&repo).Error)
	_, err := db.CreateGithubAppInstallation(batch.GithubInstallationId, 23, "owner", 1, batch.RepoFullName)
	require.NoError(t, err)
	origin := CommandProvenanceOrigin{RepositoryID: source.RepositoryID, RepoFullName: repo.RepoFullName, PrNumber: batch.PrNumber,
		BodySHA256: source.BodySHA256, CommitSHA: strings.Repeat("a", 40), Command: "apply"}
	first, firstPID := commandOriginPostgresActor(t, db)
	second, secondPID := commandOriginPostgresActor(t, db)
	for _, rejectFirst := range []bool{false, true} {
		for _, crash := range []bool{false, true} {
			batch = newCommandOriginBatch(t, db, uuid.New())
			source.CommentID++
			origin.SourceCommentID = source.CommentID
			reject := func(actor *Database) (bool, error) {
				return actor.RejectUnmergeableApply(org, origin, 23, batch.GithubInstallationId, time.Now().UTC())
			}
			admit := func(actor *Database) error {
				return actor.CreateBatchCommandOrigin(batch.ID, source, []string{"dev"})
			}
			tx := first.GormDB.Begin()
			require.NoError(t, tx.Error)
			held := &Database{GormDB: tx}
			done := make(chan error, 1)
			if rejectFirst {
				saved, err := reject(held)
				require.NoError(t, err)
				require.True(t, saved)
				go func() { done <- admit(second) }()
			} else {
				require.NoError(t, admit(held))
				go func() { _, err := reject(second); done <- err }()
			}
			commandOriginPostgresWaitForBlocker(t, db, secondPID, firstPID)
			if crash {
				require.NoError(t, tx.Rollback().Error)
			} else {
				require.NoError(t, tx.Commit().Error)
			}
			err := commandOriginPostgresResult(t, done)
			if rejectFirst && !crash {
				require.ErrorContains(t, err, "permanently rejected")
			} else {
				require.NoError(t, err)
			}
			_, receipt, err := db.GetCommandEvidence(org, &repo, batch.PrNumber, source.CommentID, true)
			require.NoError(t, err)
			shouldReject := rejectFirst != crash
			require.Equal(t, shouldReject, receipt != nil)
			allowed, err := db.AdmitApply(source)
			require.NoError(t, err)
			require.Equal(t, !shouldReject, allowed)
			if shouldReject {
				require.Error(t, db.GormDB.Exec("DELETE FROM command_decisions WHERE source_comment_id=?", source.CommentID).Error)
			} else {
				// An unsealed or partially dispatched origin must retain admission
				// even if its original batch can no longer be read.
				require.NoError(t, db.GormDB.Delete(&BatchCommandOrigin{}, "batch_id=?", batch.ID).Error)
				saved, err := reject(db)
				require.NoError(t, err)
				require.False(t, saved)
			}
		}
	}
}

func TestCommandRejectionPostgresDecisionsArePermanent(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	admitted, err := db.AdmitApply(commandSource())
	require.NoError(t, err)
	require.True(t, admitted)
	for _, statement := range []string{
		"UPDATE command_decisions SET version = 1",
		"DELETE FROM command_decisions",
		"TRUNCATE command_decisions",
		"UPDATE command_rejection_epoch SET installed_at = '2000-01-01T00:00:00Z'",
		"DELETE FROM command_rejection_epoch",
		"TRUNCATE command_rejection_epoch",
	} {
		require.ErrorContains(t, db.GormDB.Exec(statement).Error, "permanent", statement)
	}
}
