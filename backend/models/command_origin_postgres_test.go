package models

import (
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const commandOriginMigration = "20261030140000_batch_command_origins.sql"

// Each test creates and drops its own database. Set this URL to a disposable
// PostgreSQL 16 instance whose role can create databases; no default is assumed.
func commandOriginPostgresDB(t *testing.T, migrate bool) *Database {
	t.Helper()
	dsn := os.Getenv("DIGGER_COMMAND_ORIGIN_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DIGGER_COMMAND_ORIGIN_POSTGRES_DSN for PostgreSQL qualification")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, parsed.Scheme)
	query := parsed.Query()
	query.Set("statement_timeout", "10000")
	query.Set("lock_timeout", "5000")
	parsed.RawQuery = query.Encode()
	admin := commandOriginPostgresConnect(t, parsed.String())
	name := "digger41_origin_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec(`CREATE DATABASE "`+name+`"`).Error)
	t.Cleanup(func() {
		require.NoError(t, admin.Exec(`DROP DATABASE "`+name+`" WITH (FORCE)`).Error)
	})
	parsed.Path = "/" + name
	db := &Database{GormDB: commandOriginPostgresConnect(t, parsed.String())}
	files, err := filepath.Glob(filepath.Join("..", "migrations", "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.Equal(t, commandOriginMigration, filepath.Base(files[len(files)-1]))
	for _, path := range files {
		if filepath.Base(path) == commandOriginMigration && !migrate {
			continue
		}
		commandOriginApplySQL(t, db.GormDB, path)
	}
	return db
}

func commandOriginPostgresConnect(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return db
}

func commandOriginApplySQL(t *testing.T, db *gorm.DB, path string) {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return tx.Exec(string(body)).Error
	}), filepath.Base(path))
}

// This scalar model is frozen from f34a508:backend/models/scheduler.go.
// Association fields are omitted because they add no columns to a whole-row Save.
// In particular, an old writer has no workflow run ID or attempt fields to save.
type commandOriginPreviousJob struct {
	gorm.Model
	DiggerJobID                  string
	Status                       scheduler.DiggerJobStatus
	RunName                      string
	ProjectName                  string
	BatchID                      *string
	PRCommentUrl                 string
	PRCommentId                  *int64
	CheckRunId                   *string
	CheckRunUrl                  *string
	DiggerJobSummaryID           uint
	SerializedJobSpec            []byte
	SerializedReporterSpec       []byte
	SerializedCommentUpdaterSpec []byte
	SerializedLockSpec           []byte
	SerializedBackendSpec        []byte
	SerializedVcsSpec            []byte
	SerializedPolicySpec         []byte
	SerializedVariablesSpec      []byte
	TerraformOutput              string
	PlanFootprint                []byte
	WorkflowFile                 string
	WorkflowRunUrl               *string
	StatusUpdatedAt              time.Time
	ReporterType                 string `gorm:"default:'lazy'"`
}

func (commandOriginPreviousJob) TableName() string { return "digger_jobs" }

func commandOriginPostgresJob(t *testing.T, db *Database, batch DiggerBatch, project string) commandOriginPreviousJob {
	t.Helper()
	body, err := json.Marshal(scheduler.JobJson{
		ProjectName: project, Commit: batch.CommitSha, JobType: string(batch.BatchType),
		Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber,
	})
	require.NoError(t, err)
	summary := DiggerJobSummary{}
	require.NoError(t, db.GormDB.Create(&summary).Error)
	batchID, check := batch.ID.String(), "project-check-"+project
	job := commandOriginPreviousJob{
		DiggerJobID: "job-" + project, BatchID: &batchID, ProjectName: project,
		CheckRunId: &check, SerializedJobSpec: body, Status: scheduler.DiggerJobSucceeded,
		DiggerJobSummaryID: summary.ID,
	}
	require.NoError(t, db.GormDB.Create(&job).Error)
	return job
}

type commandOriginPGColumn struct {
	TableName    string
	ColumnName   string
	DataType     string
	NotNull      bool
	DefaultValue sql.NullString
}

func commandOriginPostgresColumns(t *testing.T, db *gorm.DB, schema string) []commandOriginPGColumn {
	t.Helper()
	var columns []commandOriginPGColumn
	require.NoError(t, db.Raw(`SELECT c.relname AS table_name, a.attname AS column_name,
		format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull AS not_null,
		pg_get_expr(d.adbin, d.adrelid) AS default_value
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
		WHERE n.nspname = ? AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`, schema).Scan(&columns).Error)
	return columns
}

func TestCommandOriginPostgresMigrationCompatibility(t *testing.T) {
	db := commandOriginPostgresDB(t, false)
	before := commandOriginPostgresColumns(t, db.GormDB, "public")
	legacyStatement := &gorm.Statement{DB: db.GormDB}
	require.NoError(t, legacyStatement.Parse(&commandOriginPreviousJob{}))
	var legacyColumns []string
	for _, column := range before {
		if column.TableName == "digger_jobs" {
			legacyColumns = append(legacyColumns, column.ColumnName)
		}
	}
	require.ElementsMatch(t, legacyColumns, legacyStatement.Schema.DBNames, "previous writer must cover every pre-upgrade job column")
	legacyBatch := newCommandOriginBatch(t, db, uuid.New())
	legacyJob := commandOriginPostgresJob(t, db, legacyBatch, "legacy")
	commandOriginApplySQL(t, db.GormDB, filepath.Join("..", "migrations", commandOriginMigration))

	var retained, added []commandOriginPGColumn
	for _, column := range commandOriginPostgresColumns(t, db.GormDB, "public") {
		if column.TableName == "batch_command_origins" ||
			(column.TableName == "digger_jobs" && (column.ColumnName == "workflow_run_id" || column.ColumnName == "workflow_run_attempt" || column.ColumnName == "workflow_dispatch_started_at")) {
			added = append(added, column)
		} else {
			retained = append(retained, column)
		}
	}
	require.Equal(t, before, retained, "upgrade must preserve every existing column and default")
	require.Len(t, added, 16)
	var count int64
	require.NoError(t, db.GormDB.Model(&BatchCommandOrigin{}).Count(&count).Error)
	require.Zero(t, count, "legacy batches must not receive inferred origins")
	var current DiggerJob
	require.NoError(t, db.GormDB.First(&current, legacyJob.ID).Error)
	require.Zero(t, current.WorkflowRunID)
	require.Zero(t, current.WorkflowRunAttempt)
	var nullAttempts bool
	require.NoError(t, db.GormDB.Raw(`SELECT workflow_run_id IS NULL AND workflow_run_attempt IS NULL FROM digger_jobs WHERE id = ?`, legacyJob.ID).Scan(&nullAttempts).Error)
	require.True(t, nullAttempts, "migration must leave absent attempt data unknown")

	// Generate the target tables with the actual GORM models in another schema.
	// This catches SQL drift in names, types, nullability, defaults and index order.
	require.NoError(t, db.GormDB.Transaction(func(tx *gorm.DB) error {
		require.NoError(t, tx.Exec("CREATE SCHEMA command_origin_expected").Error)
		require.NoError(t, tx.Exec("SET LOCAL search_path = command_origin_expected, public").Error)
		require.NoError(t, tx.Migrator().CreateTable(&BatchCommandOrigin{}, &DiggerJob{}))
		var expected []commandOriginPGColumn
		for _, column := range commandOriginPostgresColumns(t, tx, "command_origin_expected") {
			if column.TableName == "batch_command_origins" ||
				(column.TableName == "digger_jobs" && (column.ColumnName == "workflow_run_id" || column.ColumnName == "workflow_run_attempt" || column.ColumnName == "workflow_dispatch_started_at")) {
				expected = append(expected, column)
			}
		}
		require.Equal(t, expected, added)
		var indexes []struct{ SchemaName, Definition string }
		require.NoError(t, tx.Raw(`SELECT n.nspname AS schema_name, pg_get_indexdef(i.indexrelid) AS definition
			FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = 'idx_command_origin_source' ORDER BY n.nspname`).Scan(&indexes).Error)
		require.Len(t, indexes, 2)
		require.Equal(t, strings.ReplaceAll(indexes[0].Definition, "command_origin_expected.", "public."), indexes[1].Definition)
		return nil
	}))

	// Old writers can still insert after upgrade. New readers see unknown attempts.
	oldWriterJob := commandOriginPostgresJob(t, db, legacyBatch, "old-writer")
	current = DiggerJob{}
	require.NoError(t, db.GormDB.First(&current, oldWriterJob.ID).Error)
	require.Zero(t, current.WorkflowRunID)
	require.Zero(t, current.WorkflowRunAttempt)
	require.ErrorContains(t, db.CreateBatchCommandOrigin(legacyBatch.ID, commandSource(), []string{"legacy", "old-writer"}), "before job creation")

	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	job := commandOriginPostgresJob(t, db, batch, "a")
	current = DiggerJob{}
	require.NoError(t, db.GormDB.First(&current, job.ID).Error)
	current.WorkflowRunID, current.WorkflowRunAttempt = 991, 2
	require.NoError(t, db.GormDB.Save(&current).Error)
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)

	// DiggerBatch's columns are unchanged from f34a508. Its existing whole-model
	// Save and the frozen previous job writer must preserve newly stored evidence.
	batch.Status = scheduler.BatchJobSucceeded
	report := int64(9876)
	batch.CommentId = &report
	require.NoError(t, db.GormDB.Save(&batch).Error)
	var previous commandOriginPreviousJob
	require.NoError(t, db.GormDB.First(&previous, job.ID).Error)
	previous.RunName = "written-by-previous-version"
	require.NoError(t, db.GormDB.Save(&previous).Error)
	var after BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&after, "batch_id = ?", batch.ID).Error)
	require.Equal(t, origin, after)
	current = DiggerJob{}
	require.NoError(t, db.GormDB.First(&current, job.ID).Error)
	require.Equal(t, int64(991), current.WorkflowRunID)
	require.Equal(t, int64(2), current.WorkflowRunAttempt)
	require.Equal(t, previous.RunName, current.RunName)
	orgID := setCommandOriginScope(t, db, batch.GithubInstallationId)
	provenance, err := db.GetCommandProvenance(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
	require.NoError(t, err)
	require.True(t, provenance.Complete)
	require.Len(t, provenance.Batches, 1, "legacy batches remain absent")
	require.True(t, provenance.Batches[0].ManifestComplete)
	require.Len(t, provenance.Batches[0].Jobs, 1)
	require.Equal(t, int64(991), provenance.Batches[0].Jobs[0].WorkflowRunID)
	require.Equal(t, int64(2), provenance.Batches[0].Jobs[0].WorkflowRunAttempt)
}

// Give each actor one physical connection so pg_blocking_pids observes the
// connection executing the production method, rather than a pool's idle peer.
func commandOriginPostgresActor(t *testing.T, db *Database) (*Database, int) {
	t.Helper()
	dialector, ok := db.GormDB.Dialector.(*postgres.Dialector)
	require.True(t, ok)
	actor := commandOriginPostgresConnect(t, dialector.DSN)
	conn, err := actor.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	var pid int
	require.NoError(t, actor.Raw("SELECT pg_backend_pid()").Scan(&pid).Error)
	return &Database{GormDB: actor}, pid
}

func commandOriginPostgresWaitForBlocker(t *testing.T, db *Database, pid, blocker int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var blocked bool
		// PostgreSQL can queue a later row-lock waiter behind an earlier one.
		// Follow that chain instead of assuming both wait directly on the holder.
		err := db.GormDB.Raw(`WITH RECURSIVE blockers(pid) AS (
			SELECT unnest(pg_blocking_pids(?))
			UNION SELECT unnest(pg_blocking_pids(pid)) FROM blockers
		) SELECT EXISTS (SELECT 1 FROM blockers WHERE pid = ?)`, pid, blocker).Scan(&blocked).Error
		return err == nil && blocked
	}, 3*time.Second, 10*time.Millisecond, "pid %d must wait on pid %d", pid, blocker)
}

func commandOriginPostgresResult(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(8 * time.Second):
		t.Fatal("PostgreSQL operation did not finish after releasing its blocker")
		return nil
	}
}

func commandOriginPostgresLock(t *testing.T, db *Database, batchID uuid.UUID, origin bool) (*gorm.DB, int) {
	t.Helper()
	tx := db.GormDB.Begin()
	require.NoError(t, tx.Error)
	t.Cleanup(func() { _ = tx.Rollback().Error })
	var pid int
	require.NoError(t, tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error)
	if origin {
		var row BatchCommandOrigin
		require.NoError(t, tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, "batch_id = ?", batchID).Error)
	} else {
		var row DiggerBatch
		require.NoError(t, tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, "id = ?", batchID).Error)
	}
	return tx, pid
}

func TestCommandOriginPostgresConcurrentCreate(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	first, firstPID := commandOriginPostgresActor(t, db)
	second, secondPID := commandOriginPostgresActor(t, db)
	blocker, blockerPID := commandOriginPostgresLock(t, db, batch.ID, false)
	sources := []GitHubCommandSource{commandSource(), commandSource()}
	sources[1].CommentID++
	results := []chan error{make(chan error, 1), make(chan error, 1)}
	go func() { results[0] <- first.CreateBatchCommandOrigin(batch.ID, sources[0], []string{"a"}) }()
	go func() { results[1] <- second.CreateBatchCommandOrigin(batch.ID, sources[1], []string{"b"}) }()
	commandOriginPostgresWaitForBlocker(t, db, firstPID, blockerPID)
	commandOriginPostgresWaitForBlocker(t, db, secondPID, blockerPID)
	require.NoError(t, blocker.Rollback().Error)
	errors := []error{commandOriginPostgresResult(t, results[0]), commandOriginPostgresResult(t, results[1])}
	winner := 0
	if errors[0] != nil {
		winner = 1
	}
	require.NoError(t, errors[winner])
	require.ErrorContains(t, errors[1-winner], "duplicate key")
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.Equal(t, sources[winner].CommentID, origin.SourceCommentID)
	require.Equal(t, []string{[]string{"a", "b"}[winner]}, origin.ExpectedProjects)
	require.False(t, origin.Sealed)
}

func TestCommandOriginPostgresCreateSealLockRace(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"z", "a"}))
	z := commandOriginPostgresJob(t, db, batch, "z")
	a := commandOriginPostgresJob(t, db, batch, "a")
	sealer, sealPID := commandOriginPostgresActor(t, db)
	creator, createPID := commandOriginPostgresActor(t, db)
	blocker, blockerPID := commandOriginPostgresLock(t, db, batch.ID, true)
	sealed, created := make(chan error, 1), make(chan error, 1)
	go func() { sealed <- sealer.SealBatchCommandOrigin(batch.ID) }()
	commandOriginPostgresWaitForBlocker(t, db, sealPID, blockerPID)
	go func() {
		created <- creator.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"replacement"})
	}()
	// Sealing holds the batch while waiting for the origin. A duplicate create
	// must queue behind that batch, proving the common batch-before-origin order.
	commandOriginPostgresWaitForBlocker(t, db, createPID, sealPID)
	var pending BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&pending, "batch_id = ?", batch.ID).Error)
	require.False(t, pending.Sealed)
	require.Empty(t, pending.JobManifest)
	require.NoError(t, blocker.Rollback().Error)
	require.NoError(t, commandOriginPostgresResult(t, sealed))
	require.ErrorContains(t, commandOriginPostgresResult(t, created), "before job creation")
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.True(t, origin.Sealed)
	require.Equal(t, commandSource().CommentID, origin.SourceCommentID)
	require.Equal(t, []BatchCommandOriginJob{
		{DBID: a.ID, JobID: a.DiggerJobID, Project: "a", ProjectCheckID: *a.CheckRunId},
		{DBID: z.ID, JobID: z.DiggerJobID, Project: "z", ProjectCheckID: *z.CheckRunId},
	}, origin.JobManifest)
}

func TestCommandOriginPostgresConcurrentSeal(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	job := commandOriginPostgresJob(t, db, batch, "a")
	first, firstPID := commandOriginPostgresActor(t, db)
	second, secondPID := commandOriginPostgresActor(t, db)
	blocker, blockerPID := commandOriginPostgresLock(t, db, batch.ID, false)
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	go func() { firstResult <- first.SealBatchCommandOrigin(batch.ID) }()
	go func() { secondResult <- second.SealBatchCommandOrigin(batch.ID) }()
	commandOriginPostgresWaitForBlocker(t, db, firstPID, blockerPID)
	commandOriginPostgresWaitForBlocker(t, db, secondPID, blockerPID)
	require.NoError(t, blocker.Rollback().Error)
	one, two := commandOriginPostgresResult(t, firstResult), commandOriginPostgresResult(t, secondResult)
	if one != nil {
		one, two = two, one
	}
	require.NoError(t, one)
	require.ErrorContains(t, two, "already sealed")
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.True(t, origin.Sealed)
	require.Equal(t, []BatchCommandOriginJob{{DBID: job.ID, JobID: job.DiggerJobID, Project: "a", ProjectCheckID: *job.CheckRunId}}, origin.JobManifest)
}

func TestCommandOriginPostgresFailedSealRollsBack(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a", "b"}))
	commandOriginPostgresJob(t, db, batch, "a")
	sealer, _ := commandOriginPostgresActor(t, db)
	require.ErrorContains(t, sealer.SealBatchCommandOrigin(batch.ID), "job count")
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.False(t, origin.Sealed)
	require.Empty(t, origin.JobManifest)
	commandOriginPostgresJob(t, db, batch, "b")
	// Another connection can seal after repair: failure released both row locks.
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.True(t, origin.Sealed)
	require.Len(t, origin.JobManifest, 2)
}
