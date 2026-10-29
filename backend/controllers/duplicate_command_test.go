package controllers

import (
	"path/filepath"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGuardDuplicateCommand(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "dup.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, gdb.AutoMigrate(&models.DiggerBatch{}, &models.DiggerJob{}, &models.DiggerJobParentLink{}))
	prev := models.DB
	models.DB = &models.Database{GormDB: gdb}
	t.Cleanup(func() { models.DB = prev })

	const sha = "abcdef0123"
	for command, project := range map[scheduler.DiggerCommand]string{
		scheduler.DiggerCommandPlan:  "alpha",
		scheduler.DiggerCommandApply: "beta",
	} {
		batch, err := models.DB.CreateDiggerBatch(models.DiggerVCSGithub, 1, "o", "r", "o/r", 7, "", "b", command, nil, 0, "", false, true, nil, sha, nil, nil)
		require.NoError(t, err)
		_, err = models.DB.CreateDiggerJob(batch.ID, []byte("{}"), "w.yml", nil, nil, "lazy", project)
		require.NoError(t, err)
	}

	check := func(command scheduler.DiggerCommand, comment string, names ...string) string {
		var projects []digger_config.Project
		for _, n := range names {
			projects = append(projects, digger_config.Project{Name: n})
		}
		msg, release := guardDuplicateCommand(command, comment, "o/r", 7, sha, projects)
		release()
		return msg
	}
	plan, apply := scheduler.DiggerCommandPlan, scheduler.DiggerCommandApply

	assert.Equal(t, ":information_source: `digger plan` is already running on abcdef0123 for `alpha`, so this comment was skipped. Add `--force` to your comment to run it anyway.",
		check(plan, "digger plan", "alpha"))
	assert.Empty(t, check(plan, "digger plan", "alpha", "beta"), "a plan runs unless every project is being planned")
	assert.Contains(t, check(apply, "digger apply", "alpha", "beta"), "for `beta`, so", "an apply stops if any project is being applied")
	assert.Empty(t, check(apply, "digger apply --force", "alpha", "beta"), "--force runs anyway")
	assert.Empty(t, check(scheduler.DiggerCommandUnlock, "digger unlock", "alpha"))

	broken, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "empty.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	models.DB = &models.Database{GormDB: broken}
	assert.Contains(t, check(plan, "digger plan", "alpha"), ":x: Couldn't check", "a failed check skips the comment")

	t.Setenv("DIGGER_DUPLICATE_COMMAND_WINDOW", "0")
	assert.Empty(t, check(plan, "digger plan", "alpha"), "check disabled")
}
