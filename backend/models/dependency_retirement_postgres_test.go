package models

import (
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Frozen scalar writer from immediately previous nes-main d084622b. The
// dispatch claim is read-only in that release and is intentionally omitted.
type dependencyPreviousJob struct {
	Previous           commandOriginPreviousJob `gorm:"embedded"`
	WorkflowRunID      int64
	WorkflowRunAttempt int64
}

func (dependencyPreviousJob) TableName() string { return "digger_jobs" }

func dependencyPostgresFixture(t *testing.T) (*Database, DiggerBatch, DiggerJob, DiggerJob) {
	t.Helper()
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	parentOld := commandOriginPostgresJob(t, db, batch, "parent")
	child, err := db.CreateDiggerJob(batch.ID, parentOld.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "child")
	require.NoError(t, err)
	parent := dependencySavedJob(t, db, parentOld.ID)
	parent.Status = scheduler.DiggerJobFailed
	parent.WorkflowRunID, parent.WorkflowRunAttempt = 42665476673, 1
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	return db, batch, parent, *child
}

func TestDependencyRetirementPostgresStalePreviousWriterAndRestart(t *testing.T) {
	db, batch, _, child := dependencyPostgresFixture(t)
	var previous dependencyPreviousJob
	require.NoError(t, db.GormDB.First(&previous, child.ID).Error)
	result, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, scheduler.BatchJobFailed, result.BatchStatus)
	require.Error(t, db.GormDB.Save(&previous).Error, "old callbacks must not reopen the job")
	require.ErrorIs(t, db.UpdateDiggerJob(&child), ErrDiggerJobRetired)
	// A second connection represents a new worker using persistent state.
	restarted, _ := commandOriginPostgresActor(t, db)
	result, err = restarted.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, []string{child.DiggerJobID}, result.RetiredJobs)
	claimed, err := restarted.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Nil(t, dependencySavedJob(t, db, child.ID).WorkflowDispatchStartedAt)
}

func TestDependencyRetirementPostgresCallbackRace(t *testing.T) {
	for _, winner := range []string{"retirement", "callback"} {
		t.Run(winner, func(t *testing.T) {
			db, batch, _, child := dependencyPostgresFixture(t)
			lock := db.GormDB.Begin()
			require.NoError(t, lock.Error)
			t.Cleanup(func() { _ = lock.Rollback().Error })
			var holder int
			require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
			var row DiggerJob
			require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, child.ID).Error)
			actor, pid := commandOriginPostgresActor(t, db)
			if winner == "retirement" {
				outcomes := make(chan bool, 1)
				failures := make(chan error, 1)
				go func() { ok, err := actor.ObserveDiggerJobCallback(&child); outcomes <- ok; failures <- err }()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				// Persist using the transaction which owns the child lock.
				result, err := (&Database{GormDB: lock}).ReconcileFailedDependencies(batch.ID)
				require.NoError(t, err)
				require.Equal(t, []string{child.DiggerJobID}, result.RetiredJobs)
				require.NoError(t, lock.Commit().Error)
				select {
				case ok := <-outcomes:
					require.False(t, ok)
				case <-time.After(8 * time.Second):
					t.Fatal("callback did not return")
				}
				require.NoError(t, <-failures)
			} else {
				results := make(chan *DependencyReconciliation, 1)
				failures := make(chan error, 1)
				go func() { got, err := actor.ReconcileFailedDependencies(batch.ID); results <- got; failures <- err }()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				ok, err := (&Database{GormDB: lock}).ObserveDiggerJobCallback(&child)
				require.NoError(t, err)
				require.True(t, ok)
				require.NoError(t, lock.Commit().Error)
				select {
				case got := <-results:
					require.Empty(t, got.RetiredJobs)
					require.Equal(t, []string{child.DiggerJobID}, got.UnresolvedJobs)
				case <-time.After(8 * time.Second):
					t.Fatal("reconciler did not return")
				}
				require.NoError(t, <-failures)
			}
		})
	}
}

func TestDependencyRetirementPostgresDispatchRace(t *testing.T) {
	for _, winner := range []string{"retirement", "dispatch"} {
		t.Run(winner, func(t *testing.T) {
			db, batch, parent, child := dependencyPostgresFixture(t)
			parent.Status = scheduler.DiggerJobSucceeded
			require.NoError(t, db.UpdateDiggerJob(&parent))
			lock := db.GormDB.Begin()
			require.NoError(t, lock.Error)
			t.Cleanup(func() { _ = lock.Rollback().Error })
			var holder int
			require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
			var row DiggerJob
			require.NoError(t, lock.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, child.ID).Error)
			actor, pid := commandOriginPostgresActor(t, db)
			if winner == "retirement" {
				results := make(chan bool, 1)
				failures := make(chan error, 1)
				go func() { ok, err := actor.ClaimDiggerJobDispatch(&child); results <- ok; failures <- err }()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				parent.Status = scheduler.DiggerJobFailed
				lockedDB := &Database{GormDB: lock}
				require.NoError(t, lockedDB.UpdateDiggerJob(&parent))
				repaired, err := lockedDB.ReconcileFailedDependencies(batch.ID)
				require.NoError(t, err)
				require.Equal(t, []string{child.DiggerJobID}, repaired.RetiredJobs)
				require.NoError(t, lock.Commit().Error)
				select {
				case ok := <-results:
					require.False(t, ok)
				case <-time.After(8 * time.Second):
					t.Fatal("blocked dispatch claim did not return")
				}
				require.NoError(t, <-failures)
			} else {
				lockedDB := &Database{GormDB: lock}
				ok, err := lockedDB.ClaimDiggerJobDispatch(&child)
				require.NoError(t, err)
				require.True(t, ok)
				parent.Status = scheduler.DiggerJobFailed
				require.NoError(t, lockedDB.UpdateDiggerJob(&parent))
				results := make(chan *DependencyReconciliation, 1)
				failures := make(chan error, 1)
				go func() { got, err := actor.ReconcileFailedDependencies(batch.ID); results <- got; failures <- err }()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				require.NoError(t, lock.Commit().Error)
				select {
				case got := <-results:
					require.Empty(t, got.RetiredJobs)
					require.Equal(t, []string{child.DiggerJobID}, got.UnresolvedJobs)
				case <-time.After(8 * time.Second):
					t.Fatal("blocked reconciler did not return")
				}
				require.NoError(t, <-failures)
				saved := dependencySavedJob(t, db, child.ID)
				require.Nil(t, saved.DependencyRetiredAt)
				require.NotNil(t, saved.WorkflowDispatchStartedAt)
			}
		})
	}
}

func TestDependencyRetirementPostgresMigrationPreservesPreviousRelease(t *testing.T) {
	db := commandOriginPostgresDB(t, false)
	commandOriginApplySQL(t, db.GormDB, "../migrations/20261030140000_batch_command_origins.sql")
	commandOriginApplySQL(t, db.GormDB, "../migrations/20261101120000.sql")
	batch := newCommandOriginBatch(t, db, uuid.New())
	old := commandOriginPostgresJob(t, db, batch, "legacy")
	before := commandOriginPostgresColumns(t, db.GormDB, "public")
	commandOriginApplySQL(t, db.GormDB, "../migrations/20261112040000_dependency_retirement.sql")
	after := commandOriginPostgresColumns(t, db.GormDB, "public")
	var retained, added []commandOriginPGColumn
	for _, column := range after {
		if column.TableName == "digger_jobs" && (column.ColumnName == "callback_revision" || column.ColumnName == "dispatch_protocol_version" || column.ColumnName == "dependency_retired_at" || column.ColumnName == "dependency_failure_job_id") {
			added = append(added, column)
		} else {
			retained = append(retained, column)
		}
	}
	require.Equal(t, before, retained)
	require.Len(t, added, 4)
	require.NoError(t, db.GormDB.Transaction(func(tx *gorm.DB) error {
		require.NoError(t, tx.Exec("CREATE SCHEMA retirement_expected").Error)
		require.NoError(t, tx.Exec("SET LOCAL search_path = retirement_expected, public").Error)
		require.NoError(t, tx.Migrator().CreateTable(&DiggerJob{}))
		var expected []commandOriginPGColumn
		for _, column := range commandOriginPostgresColumns(t, tx, "retirement_expected") {
			if column.TableName == "digger_jobs" && (column.ColumnName == "callback_revision" || column.ColumnName == "dispatch_protocol_version" || column.ColumnName == "dependency_retired_at" || column.ColumnName == "dependency_failure_job_id") {
				expected = append(expected, column)
			}
		}
		require.Equal(t, expected, added, "SQL migration must match new model columns and defaults")
		return nil
	}))
	saved := dependencySavedJob(t, db, old.ID)
	require.Zero(t, saved.DispatchProtocolVersion, "migration cannot fabricate historical certainty")
	require.Equal(t, old.Status, saved.Status)
	// The previous binary can read/write ordinary rows after an upgrade.
	var previous dependencyPreviousJob
	require.NoError(t, db.GormDB.First(&previous, old.ID).Error)
	previous.Previous.RunName = "previous-release"
	require.NoError(t, db.GormDB.Save(&previous).Error)
	// Its new inserts remain legacy, rather than inheriting the opt-in marker.
	oldInsert := commandOriginPostgresJob(t, db, batch, "previous-insert")
	require.Zero(t, dependencySavedJob(t, db, oldInsert.ID).DispatchProtocolVersion)
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	current, err := db.CreateDiggerJob(batch.ID, old.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "current")
	require.NoError(t, err)
	require.Equal(t, 1, dependencySavedJob(t, db, current.ID).DispatchProtocolVersion)
}
