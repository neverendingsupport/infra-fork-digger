package models

import (
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const commandRejectionMigration = "20261120120000_command_rejections.sql"

// The rejection matches commandSource() and newCommandOriginBatch's identities.
func applyRejection(head string) ApplyRejection {
	return ApplyRejection{Reason: RejectionApplyRequirements, RepoFullName: "owner/repo", PrNumber: 41, CommitSHA: head, GithubAppID: 5, GithubInstallationID: 77}
}

func newApplyOriginBatch(t *testing.T, db *Database) DiggerBatch {
	t.Helper()
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.GormDB.Model(&batch).Update("batch_type", scheduler.DiggerCommandApply).Error)
	batch.BatchType = scheduler.DiggerCommandApply
	return batch
}

func rowCount(t *testing.T, db *Database, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.GormDB.Table(table).Count(&n).Error)
	return n
}

// An origin inserted the way a replica without this change writes it.
func previousProducerOrigin(tx *gorm.DB, batch DiggerBatch, source GitHubCommandSource) error {
	return tx.Exec(`INSERT INTO batch_command_origins
		(batch_id, repository_id, repo_full_name, pr_number, source_comment_id, body_sha256, commit_sha, command, github_installation_id, aggregate_check_id, expected_projects)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'aggregate-check', '["dev"]')`,
		batch.ID, source.RepositoryID, batch.RepoFullName, batch.PrNumber, source.CommentID, source.BodySHA256, batch.CommitSha, batch.BatchType, batch.GithubInstallationId).Error
}

func receipts(t *testing.T, db *Database) []CommandSourceDisposition {
	t.Helper()
	var rows []CommandSourceDisposition
	require.NoError(t, db.GormDB.Where("disposition = ?", CommandSourceRejected).Find(&rows).Error)
	return rows
}

func TestCommandRejectionPostgresFencesEveryLaterBatch(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	require.NoError(t, db.RecordApplyRejection(commandSource(), applyRejection("checked-head")))

	err := db.CreateBatchCommandOrigin(newApplyOriginBatch(t, db).ID, commandSource(), []string{"dev"})
	require.ErrorIs(t, err, ErrCommandSourceRejected)
	require.ErrorContains(t, previousProducerOrigin(db.GormDB, newApplyOriginBatch(t, db), commandSource()), "rejected before dispatch")
	require.Zero(t, rowCount(t, db, "batch_command_origins"))

	// A delayed delivery after the head moved keeps the first receipt.
	require.NoError(t, db.RecordApplyRejection(commandSource(), applyRejection("newer-head")))
	got := receipts(t, db)
	require.Len(t, got, 1)
	require.Equal(t, "checked-head", got[0].CommitSHA)

	rejected, err := db.IsCommandSourceRejected(commandSource())
	require.NoError(t, err)
	require.True(t, rejected)

	// A new comment is a new source and can still dispatch.
	retry := commandSource()
	retry.CommentID++
	rejected, err = db.IsCommandSourceRejected(retry)
	require.NoError(t, err)
	require.False(t, rejected)
	require.NoError(t, db.CreateBatchCommandOrigin(newApplyOriginBatch(t, db).ID, retry, []string{"dev"}))
}

func TestCommandRejectionPostgresKeepsDispatchedWork(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	require.NoError(t, db.CreateBatchCommandOrigin(newApplyOriginBatch(t, db).ID, commandSource(), []string{"dev"}))
	require.ErrorIs(t, db.RecordApplyRejection(commandSource(), applyRejection("checked-head")), ErrCommandSourceDispatched)
	require.Empty(t, receipts(t, db))

	// A second batch for the same comment, such as a forced retry, still works.
	require.NoError(t, db.CreateBatchCommandOrigin(newApplyOriginBatch(t, db).ID, commandSource(), []string{"dev"}))
}

// Plan comments can't be rejected, so their origins add no rows.
func TestCommandRejectionPostgresIgnoresPlanOrigins(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	require.NoError(t, db.CreateBatchCommandOrigin(newCommandOriginBatch(t, db, uuid.New()).ID, commandSource(), []string{"dev"}))
	require.Zero(t, rowCount(t, db, "command_source_dispositions"))
}

func TestCommandRejectionPostgresBackfillsEarlierApplyOrigins(t *testing.T) {
	db := commandOriginPostgresDBBefore(t, commandRejectionMigration)
	planned := commandSource()
	planned.CommentID++
	require.NoError(t, previousProducerOrigin(db.GormDB, newApplyOriginBatch(t, db), commandSource()))
	require.NoError(t, previousProducerOrigin(db.GormDB, newCommandOriginBatch(t, db, uuid.New()), planned))
	commandOriginApplySQL(t, db.GormDB, "../migrations/"+commandRejectionMigration)
	require.ErrorIs(t, db.RecordApplyRejection(commandSource(), applyRejection("checked-head")), ErrCommandSourceDispatched)
	require.Equal(t, int64(1), rowCount(t, db, "command_source_dispositions"))
}

func TestCommandRejectionPostgresIsImmutable(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	require.NoError(t, db.RecordApplyRejection(commandSource(), applyRejection("checked-head")))
	for _, statement := range []string{
		"UPDATE command_source_dispositions SET commit_sha = 'other'",
		"DELETE FROM command_source_dispositions",
		"TRUNCATE command_source_dispositions",
	} {
		require.ErrorContains(t, db.GormDB.Exec(statement).Error, "immutable", statement)
	}
	// Only a rejection carries a receipt, and a rejection always has one.
	other := commandSource()
	other.CommentID++
	partial := rejectionRow(other, applyRejection("checked-head"))
	partial.GithubAppID = 0
	require.ErrorContains(t, db.GormDB.Exec(`INSERT INTO command_source_dispositions
		(repository_id, source_comment_id, disposition, receipt_version, reason, repo_full_name, pr_number, body_sha256, commit_sha, github_installation_id)
		VALUES (?, ?, 'rejected', 1, ?, ?, ?, ?, ?, ?)`, partial.RepositoryID, partial.SourceCommentID, partial.Reason,
		partial.RepoFullName, partial.PrNumber, partial.BodySHA256, partial.CommitSHA, partial.GithubInstallationID).Error, "check constraint")
	require.ErrorContains(t, db.GormDB.Exec(`INSERT INTO command_source_dispositions
		(repository_id, source_comment_id, disposition, reason) VALUES (?, ?, 'batch_created', 'apply_requirements')`,
		other.RepositoryID, other.CommentID).Error, "check constraint")
}

func TestCommandRejectionPostgresSerializesWriters(t *testing.T) {
	for _, tc := range []struct {
		name    string
		holder  func(t *testing.T, tx *gorm.DB, db *Database) error
		waiter  func(t *testing.T, db *Database) error
		outcome func(t *testing.T, db *Database, err error)
	}{
		{
			name: "rejection_then_batch",
			holder: func(_ *testing.T, tx *gorm.DB, _ *Database) error {
				return recordApplyRejection(tx, rejectionRow(commandSource(), applyRejection("checked-head")))
			},
			waiter: func(t *testing.T, db *Database) error {
				return db.CreateBatchCommandOrigin(newApplyOriginBatch(t, db).ID, commandSource(), []string{"dev"})
			},
			outcome: func(t *testing.T, db *Database, err error) {
				require.ErrorIs(t, err, ErrCommandSourceRejected)
				require.Zero(t, rowCount(t, db, "batch_command_origins"))
			},
		},
		{
			name: "batch_then_rejection",
			holder: func(t *testing.T, tx *gorm.DB, db *Database) error {
				return previousProducerOrigin(tx, newApplyOriginBatch(t, db), commandSource())
			},
			waiter: func(_ *testing.T, db *Database) error {
				return db.RecordApplyRejection(commandSource(), applyRejection("checked-head"))
			},
			outcome: func(t *testing.T, db *Database, err error) {
				require.ErrorIs(t, err, ErrCommandSourceDispatched)
				require.Empty(t, receipts(t, db))
			},
		},
		{
			name: "two_replicas_reject",
			holder: func(_ *testing.T, tx *gorm.DB, _ *Database) error {
				return recordApplyRejection(tx, rejectionRow(commandSource(), applyRejection("first-replica")))
			},
			waiter: func(_ *testing.T, db *Database) error {
				return db.RecordApplyRejection(commandSource(), applyRejection("second-replica"))
			},
			outcome: func(t *testing.T, db *Database, err error) {
				require.NoError(t, err)
				got := receipts(t, db)
				require.Len(t, got, 1)
				require.Equal(t, "first-replica", got[0].CommitSHA)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			// Hold the first writer uncommitted, so the second must wait on the
			// disposition's primary key and then see the committed outcome.
			holder := db.GormDB.Begin()
			require.NoError(t, holder.Error)
			t.Cleanup(func() { _ = holder.Rollback().Error })
			var holderPID int
			require.NoError(t, holder.Raw("SELECT pg_backend_pid()").Scan(&holderPID).Error)
			require.NoError(t, tc.holder(t, holder, db))
			actor, actorPID := commandOriginPostgresActor(t, db)
			result := make(chan error, 1)
			go func() { result <- tc.waiter(t, actor) }()
			commandOriginPostgresWaitForBlocker(t, db, actorPID, holderPID)
			require.NoError(t, holder.Commit().Error)
			tc.outcome(t, db, commandOriginPostgresResult(t, result))
		})
	}
}

func TestCommandRejectionPostgresReadIsScopedToTheOrganisation(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	org := Organisation{Name: "owner", ExternalId: "owner-id", ExternalSource: "test"}
	other := Organisation{Name: "other", ExternalId: "other-id", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&org).Error)
	require.NoError(t, db.GormDB.Create(&other).Error)
	_, err := db.CreateGithubInstallationLink(&org, 77)
	require.NoError(t, err)

	empty, err := db.GetCommandRejection(org.ID, "owner/repo", 41, commandSource().CommentID)
	require.NoError(t, err)
	require.Equal(t, &CommandRejectionProjection{Version: 1}, empty)

	require.NoError(t, db.RecordApplyRejection(commandSource(), applyRejection("checked-head")))
	got, err := db.GetCommandRejection(org.ID, "owner/repo", 41, commandSource().CommentID)
	require.NoError(t, err)
	require.NotNil(t, got.Receipt)
	require.Equal(t, CommandRejectionReceipt{
		Version: 1, Kind: commandRejectionKind, Reason: RejectionApplyRequirements, DispatchFenced: true,
		Origin: CommandProvenanceOrigin{
			RepositoryID: 123, RepoFullName: "owner/repo", PrNumber: 41, SourceCommentID: 456,
			BodySHA256: commandSource().BodySHA256, CommitSHA: "checked-head", Command: scheduler.DiggerCommandApply,
		},
		GithubAppID: 5, GithubInstallationID: 77, RejectedAt: got.Receipt.RejectedAt,
	}, *got.Receipt)

	foreign, err := db.GetCommandRejection(other.ID, "owner/repo", 41, commandSource().CommentID)
	require.NoError(t, err)
	require.Nil(t, foreign.Receipt)
}
