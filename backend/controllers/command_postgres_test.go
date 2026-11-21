package controllers

import (
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const commandFixtureMigration = "20261030140000_batch_command_origins.sql"

// Each test creates and drops its own database. Set this URL to a disposable
// PostgreSQL 16 instance whose role can create databases; no default is assumed.
func CommandPostgresDB(t *testing.T) *models.Database {
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
	admin := commandFixturePostgresConnect(t, parsed.String())
	name := "digger41_origin_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec(`CREATE DATABASE "`+name+`"`).Error)
	t.Cleanup(func() {
		require.NoError(t, admin.Exec(`DROP DATABASE "`+name+`" WITH (FORCE)`).Error)
	})
	parsed.Path = "/" + name
	db := &models.Database{GormDB: commandFixturePostgresConnect(t, parsed.String())}
	files, err := filepath.Glob(filepath.Join("..", "migrations", "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.True(t, slices.ContainsFunc(files, func(f string) bool { return filepath.Base(f) == commandFixtureMigration }))
	for _, path := range files {
		commandFixtureApplySQL(t, db.GormDB, path)
	}
	return db
}

func commandFixturePostgresConnect(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return db
}

func commandFixtureApplySQL(t *testing.T, db *gorm.DB, path string) {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return tx.Exec(string(body)).Error
	}), filepath.Base(path))
}
