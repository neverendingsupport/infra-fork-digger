package models

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newCommandOriginDB(t *testing.T) *Database {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "command-origin.db")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, gdb.AutoMigrate(&DiggerBatch{}, &DiggerJob{}, &BatchCommandOrigin{}, &Organisation{}, &GithubAppInstallationLink{}))
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return &Database{GormDB: gdb}
}

func commandSource() GitHubCommandSource {
	return GitHubCommandSource{RepositoryID: 123, CommentID: 456, BodySHA256: strings.Repeat("a", 64)}
}

func newCommandOriginBatch(t *testing.T, db *Database, id uuid.UUID) DiggerBatch {
	t.Helper()
	check, report := "aggregate-check", int64(999)
	batch := DiggerBatch{
		ID: id, VCS: DiggerVCSGithub, PrNumber: 41, CommitSha: "exact-head",
		CommentId: &report, CheckRunId: &check, Status: scheduler.BatchJobCreated,
		GithubInstallationId: 77, RepoFullName: "owner/repo", BatchType: scheduler.DiggerCommandPlan,
	}
	require.NoError(t, db.GormDB.Create(&batch).Error)
	return batch
}

func newCommandOriginJob(t *testing.T, db *Database, batch DiggerBatch, project string) DiggerJob {
	t.Helper()
	serialized, err := json.Marshal(scheduler.JobJson{
		ProjectName: project, Commit: batch.CommitSha, JobType: string(batch.BatchType),
		Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber,
		BackendJobToken: "never-return-this-token", RunEnvVars: map[string]string{"secret": "never-return-this-env"},
	})
	require.NoError(t, err)
	batchID, check, workflow := batch.ID.String(), "check-"+project, "https://github.com/owner/repo/actions/runs/991/attempts/2"
	job := DiggerJob{
		DiggerJobID: "job-" + project, BatchID: &batchID, ProjectName: project,
		CheckRunId: &check, SerializedJobSpec: serialized, Status: scheduler.DiggerJobSucceeded,
		WorkflowRunUrl: &workflow, TerraformOutput: "never-return-this-output",
	}
	require.NoError(t, db.GormDB.Create(&job).Error)
	return job
}

func setCommandOriginScope(t *testing.T, db *Database, installationID int64) uint {
	t.Helper()
	org := Organisation{Name: fmt.Sprintf("org-%d", installationID), ExternalSource: "test", ExternalId: fmt.Sprint(installationID)}
	require.NoError(t, db.GormDB.Create(&org).Error)
	link := GithubAppInstallationLink{OrganisationId: org.ID, GithubInstallationId: installationID, Status: GithubAppInstallationLinkActive}
	require.NoError(t, db.GormDB.Create(&link).Error)
	return org.ID
}

func TestCreateBatchCommandOriginImmutableSource(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	source := commandSource()
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, source, []string{"z", "a", "a"}))
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.Equal(t, []string{"a", "z"}, origin.ExpectedProjects)
	require.Equal(t, source.CommentID, origin.SourceCommentID)
	require.NotEqual(t, *batch.CommentId, origin.SourceCommentID)
	require.Equal(t, source.BodySHA256, origin.BodySHA256)
	require.Equal(t, source.RepositoryID, origin.RepositoryID)
	require.False(t, origin.Sealed)

	changed := source
	changed.CommentID++
	require.Error(t, db.CreateBatchCommandOrigin(batch.ID, changed, []string{"replacement"}))
	batch.Status = scheduler.BatchJobSucceeded
	require.NoError(t, db.GormDB.Save(&batch).Error)
	var after BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&after, "batch_id = ?", batch.ID).Error)
	require.Equal(t, origin, after)

	// Ordinary model updates cannot rewrite create-only source fields.
	require.NoError(t, db.GormDB.Model(&origin).Update("SourceCommentID", changed.CommentID).Error)
	require.NoError(t, db.GormDB.First(&after, "batch_id = ?", batch.ID).Error)
	require.Equal(t, source.CommentID, after.SourceCommentID)
}

func TestCreateBatchCommandOriginRejectsInvalidSourceAndBatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*DiggerBatch, *GitHubCommandSource, *[]string)
	}{
		{"repository_id", func(_ *DiggerBatch, s *GitHubCommandSource, _ *[]string) { s.RepositoryID = 0 }},
		{"comment_id", func(_ *DiggerBatch, s *GitHubCommandSource, _ *[]string) { s.CommentID = -1 }},
		{"hash", func(_ *DiggerBatch, s *GitHubCommandSource, _ *[]string) { s.BodySHA256 = "not-a-digest" }},
		{"vcs", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.VCS = DiggerVCSGitlab }},
		{"command", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.BatchType = "destroy" }},
		{"head", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.CommitSha = "" }},
		{"repo", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.RepoFullName = "" }},
		{"pr", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.PrNumber = 0 }},
		{"installation", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.GithubInstallationId = 0 }},
		{"check", func(b *DiggerBatch, _ *GitHubCommandSource, _ *[]string) { b.CheckRunId = nil }},
		{"empty_projects", func(_ *DiggerBatch, _ *GitHubCommandSource, p *[]string) { *p = nil }},
		{"blank_project", func(_ *DiggerBatch, _ *GitHubCommandSource, p *[]string) { *p = []string{" "} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newCommandOriginDB(t)
			batch := newCommandOriginBatch(t, db, uuid.New())
			source, projects := commandSource(), []string{"a"}
			tc.mutate(&batch, &source, &projects)
			require.NoError(t, db.GormDB.Save(&batch).Error)
			require.Error(t, db.CreateBatchCommandOrigin(batch.ID, source, projects))
			var count int64
			require.NoError(t, db.GormDB.Model(&BatchCommandOrigin{}).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestSealBatchCommandOriginExactManifest(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"z", "a"}))
	z := newCommandOriginJob(t, db, batch, "z")
	a := newCommandOriginJob(t, db, batch, "a")
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.True(t, origin.Sealed)
	require.Equal(t, []BatchCommandOriginJob{
		{DBID: a.ID, JobID: a.DiggerJobID, Project: "a", ProjectCheckID: *a.CheckRunId},
		{DBID: z.ID, JobID: z.DiggerJobID, Project: "z", ProjectCheckID: *z.CheckRunId},
	}, origin.JobManifest)
	require.Error(t, db.SealBatchCommandOrigin(batch.ID))
	var after BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&after, "batch_id = ?", batch.ID).Error)
	require.Equal(t, origin, after)
}

func TestSealBatchCommandOriginRejectsIdentitySubstitution(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *Database, DiggerBatch, []DiggerJob)
	}{
		{"missing_job", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Unscoped().Delete(&j[1]).Error)
		}},
		{"deleted_job", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Delete(&j[1]).Error)
		}},
		{"extra_project", func(t *testing.T, db *Database, b DiggerBatch, _ []DiggerJob) { newCommandOriginJob(t, db, b, "extra") }},
		{"mixed_project", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("project_name", "a").Error)
		}},
		{"duplicate_project", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Updates(map[string]interface{}{
				"project_name": "a", "serialized_job_spec": []byte(`{"projectName":"a","commit":"exact-head"}`),
			}).Error)
		}},
		{"spec_project", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("serialized_job_spec", []byte(`{"projectName":"other","commit":"exact-head"}`)).Error)
		}},
		{"mixed_head", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("serialized_job_spec", []byte(`{"projectName":"b","commit":"old-head"}`)).Error)
		}},
		{"missing_head", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("serialized_job_spec", []byte(`{"projectName":"b"}`)).Error)
		}},
		{"malformed_spec", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("serialized_job_spec", []byte(`secret-not-json`)).Error)
		}},
		{"missing_job_id", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("digger_job_id", "").Error)
		}},
		{"duplicate_job_id", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("digger_job_id", j[0].DiggerJobID).Error)
		}},
		{"missing_check", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("check_run_id", nil).Error)
		}},
		{"duplicate_check", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("check_run_id", j[0].CheckRunId).Error)
		}},
		{"aggregate_check_reuse", func(t *testing.T, db *Database, b DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("check_run_id", b.CheckRunId).Error)
		}},
		{"foreign_batch", func(t *testing.T, db *Database, _ DiggerBatch, j []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j[1]).Update("batch_id", uuid.New().String()).Error)
		}},
		{"batch_head_changed", func(t *testing.T, db *Database, b DiggerBatch, _ []DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("commit_sha", "replacement").Error)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newCommandOriginDB(t)
			batch := newCommandOriginBatch(t, db, uuid.New())
			require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a", "b"}))
			jobs := []DiggerJob{newCommandOriginJob(t, db, batch, "a"), newCommandOriginJob(t, db, batch, "b")}
			tc.mutate(t, db, batch, jobs)
			require.Error(t, db.SealBatchCommandOrigin(batch.ID))
			var origin BatchCommandOrigin
			require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
			require.False(t, origin.Sealed)
			require.Empty(t, origin.JobManifest)
		})
	}
}

func TestSealBatchCommandOriginDoesNotCreateLegacyOrigin(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	newCommandOriginJob(t, db, batch, "a")
	require.ErrorIs(t, db.SealBatchCommandOrigin(batch.ID), gorm.ErrRecordNotFound)
}

func TestCreateAndSealApplyCommandOrigin(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	batch.BatchType = scheduler.DiggerCommandApply
	require.NoError(t, db.GormDB.Save(&batch).Error)
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	newCommandOriginJob(t, db, batch, "a")
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.Equal(t, scheduler.DiggerCommandApply, origin.Command)
	require.True(t, origin.Sealed)
}

func TestCreateBatchCommandOriginRejectsDeletedJobBackfill(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	job := newCommandOriginJob(t, db, batch, "a")
	require.NoError(t, db.GormDB.Delete(&job).Error)
	require.Error(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
}
