package models

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetCommandProvenanceSourceToEvidenceBoundary(t *testing.T) {
	db := newCommandOriginDB(t)
	orgID := setCommandOriginScope(t, db, 77)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	job := newCommandOriginJob(t, db, batch, "a")
	require.NoError(t, db.GormDB.Model(&job).Updates(map[string]interface{}{"workflow_run_id": 991, "workflow_run_attempt": 2}).Error)
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	require.NoError(t, db.GormDB.Model(&batch).Update("status", scheduler.BatchJobSucceeded).Error)
	result, err := db.GetCommandProvenance(orgID, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Equal(t, 1, result.Version)
	require.Len(t, result.Batches, 1)
	got := result.Batches[0]
	require.True(t, got.ManifestComplete)
	require.Equal(t, batch.ID.String(), got.BatchID)
	require.Equal(t, scheduler.BatchJobSucceeded, got.Status)
	require.Equal(t, *batch.CheckRunId, got.AggregateCheckID)
	require.Equal(t, CommandProvenanceOrigin{RepositoryID: 123, RepoFullName: batch.RepoFullName, PrNumber: 41, SourceCommentID: 456, BodySHA256: commandSource().BodySHA256, CommitSHA: batch.CommitSha, Command: scheduler.DiggerCommandPlan}, got.Origin)
	require.Equal(t, []CommandProvenanceJob{{DBID: job.ID, JobID: job.DiggerJobID, Project: "a", ProjectCheckID: *job.CheckRunId, Status: scheduler.DiggerJobSucceeded, WorkflowURL: *job.WorkflowRunUrl, WorkflowRunID: 991, WorkflowRunAttempt: 2}}, got.Jobs)

	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	for _, secret := range []string{"never-return-this-token", "never-return-this-env", "never-return-this-output", "serialized", "BackendJobToken"} {
		require.NotContains(t, string(encoded), secret)
	}
	for _, field := range []string{"manifest_complete", "source_comment_id", "workflow_run_id", "workflow_run_attempt"} {
		require.Contains(t, string(encoded), field)
	}
	// A report comment, unrelated source comment, PR or repo cannot select this
	// evidence, even though successful jobs exist at the same head.
	for _, query := range []struct {
		repo    string
		pr      int
		comment int64
	}{
		{batch.RepoFullName, 41, *batch.CommentId},
		{batch.RepoFullName, 41, 457},
		{batch.RepoFullName, 42, 456},
		{"other/repo", 41, 456},
	} {
		missing, err := db.GetCommandProvenance(orgID, query.repo, query.pr, query.comment)
		require.NoError(t, err)
		require.True(t, missing.Complete)
		require.Empty(t, missing.Batches)
	}
}

func TestGetCommandProvenanceStrictOrganisationScope(t *testing.T) {
	for _, scope := range []string{"foreign_org", "inactive_link", "deleted_link", "deleted_org"} {
		t.Run(scope, func(t *testing.T) {
			db := newCommandOriginDB(t)
			orgID := setCommandOriginScope(t, db, 77)
			batch := newCommandOriginBatch(t, db, uuid.New())
			require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
			newCommandOriginJob(t, db, batch, "a")
			require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
			switch scope {
			case "foreign_org":
				orgID = setCommandOriginScope(t, db, 88)
			case "inactive_link":
				require.NoError(t, db.GormDB.Model(&GithubAppInstallationLink{}).Where("organisation_id = ?", orgID).Update("status", GithubAppInstallationLinkInactive).Error)
			case "deleted_link":
				require.NoError(t, db.GormDB.Where("organisation_id = ?", orgID).Delete(&GithubAppInstallationLink{}).Error)
			case "deleted_org":
				require.NoError(t, db.GormDB.Delete(&Organisation{}, orgID).Error)
			}
			got, err := db.GetCommandProvenance(orgID, batch.RepoFullName, 41, 456)
			require.NoError(t, err)
			require.Empty(t, got.Batches)
		})
	}
}

func TestGetCommandProvenanceLegacyAndUnsealedAreNotPositiveEvidence(t *testing.T) {
	db := newCommandOriginDB(t)
	orgID := setCommandOriginScope(t, db, 77)
	batch := newCommandOriginBatch(t, db, uuid.New())
	newCommandOriginJob(t, db, batch, "a")
	legacy, err := db.GetCommandProvenance(orgID, batch.RepoFullName, 41, 456)
	require.NoError(t, err)
	require.True(t, legacy.Complete)
	require.Empty(t, legacy.Batches)

	// Existing automatic/legacy jobs cannot acquire a source retrospectively.
	require.Error(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	batch = newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	newCommandOriginJob(t, db, batch, "a")
	unsealed, err := db.GetCommandProvenance(orgID, batch.RepoFullName, 41, 456)
	require.NoError(t, err)
	require.Len(t, unsealed.Batches, 1)
	require.False(t, unsealed.Batches[0].ManifestComplete)
	require.Empty(t, unsealed.Batches[0].Jobs)

	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	sealed, err := db.GetCommandProvenance(orgID, batch.RepoFullName, 41, 456)
	require.NoError(t, err)
	require.True(t, sealed.Batches[0].ManifestComplete)
	require.Len(t, sealed.Batches[0].Jobs, 1)
	// Even an attempt-shaped workflow URL cannot fill absent legacy attempt data.
	require.NotEmpty(t, sealed.Batches[0].Jobs[0].WorkflowURL)
	require.Zero(t, sealed.Batches[0].Jobs[0].WorkflowRunID)
	require.Zero(t, sealed.Batches[0].Jobs[0].WorkflowRunAttempt)
}

func TestGetCommandProvenanceDetectsSealedIdentityDrift(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*testing.T, *Database, DiggerBatch, DiggerJob)
		unknownJob bool
	}{
		{"missing", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Unscoped().Delete(&j).Error)
		}, true},
		{"deleted", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Delete(&j).Error)
		}, true},
		{"extra", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) { newCommandOriginJob(t, db, b, "extra") }, false},
		{"replacement_db_id", func(t *testing.T, db *Database, b DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Unscoped().Delete(&j).Error)
			newCommandOriginJob(t, db, b, "a")
		}, true},
		{"job_id", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j).Update("digger_job_id", "replacement").Error)
		}, true},
		{"check_id", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j).Update("check_run_id", "replacement").Error)
		}, true},
		{"project", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j).Update("project_name", "replacement").Error)
		}, true},
		{"spec_head", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j).Update("serialized_job_spec", []byte(`{"projectName":"a","commit":"old-head"}`)).Error)
		}, true},
		{"spec_project", func(t *testing.T, db *Database, _ DiggerBatch, j DiggerJob) {
			require.NoError(t, db.GormDB.Model(&j).Update("serialized_job_spec", []byte(`{"projectName":"other","commit":"exact-head"}`)).Error)
		}, true},
		{"batch_head", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("commit_sha", "other").Error)
		}, true},
		{"batch_repo", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("repo_full_name", "other/repo").Error)
		}, true},
		{"batch_pr", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("pr_number", 42).Error)
		}, true},
		{"batch_command", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("batch_type", scheduler.DiggerCommandApply).Error)
		}, true},
		{"batch_check", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("check_run_id", "replacement").Error)
		}, true},
		{"batch_installation", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Model(&b).Update("github_installation_id", 88).Error)
		}, true},
		{"deleted_batch", func(t *testing.T, db *Database, b DiggerBatch, _ DiggerJob) {
			require.NoError(t, db.GormDB.Delete(&b).Error)
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newCommandOriginDB(t)
			orgID := setCommandOriginScope(t, db, 77)
			batch := newCommandOriginBatch(t, db, uuid.New())
			require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
			job := newCommandOriginJob(t, db, batch, "a")
			require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
			tc.mutate(t, db, batch, job)
			result, err := db.GetCommandProvenance(orgID, batch.RepoFullName, 41, 456)
			require.NoError(t, err)
			require.True(t, result.Complete, "query completeness differs from manifest completeness")
			require.Len(t, result.Batches, 1, "mutating batch fields cannot hide its origin")
			got := result.Batches[0]
			require.False(t, got.ManifestComplete)
			require.Equal(t, batch.CommitSha, got.Origin.CommitSHA)
			require.Len(t, got.Jobs, 1)
			require.Equal(t, job.ID, got.Jobs[0].DBID)
			require.Equal(t, job.DiggerJobID, got.Jobs[0].JobID)
			if tc.unknownJob {
				require.Zero(t, got.Jobs[0].Status)
				require.Empty(t, got.Jobs[0].WorkflowURL)
			}
		})
	}
}

func TestGetCommandProvenanceStableOrderAndBatchBound(t *testing.T) {
	db := newCommandOriginDB(t)
	orgID := setCommandOriginScope(t, db, 77)
	for i := MaxCommandProvenanceBatches; i >= 1; i-- {
		id := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
		batch := newCommandOriginBatch(t, db, id)
		require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"z", "a"}))
		newCommandOriginJob(t, db, batch, "z")
		newCommandOriginJob(t, db, batch, "a")
		require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	}
	result, err := db.GetCommandProvenance(orgID, "owner/repo", 41, 456)
	require.NoError(t, err)
	require.Len(t, result.Batches, MaxCommandProvenanceBatches)
	for i, batch := range result.Batches {
		require.Equal(t, fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1), batch.BatchID)
		require.True(t, batch.ManifestComplete)
		require.Equal(t, "a", batch.Jobs[0].Project)
		require.Equal(t, "z", batch.Jobs[1].Project)
	}
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	result, err = db.GetCommandProvenance(orgID, "owner/repo", 41, 456)
	require.ErrorIs(t, err, ErrProvenanceTooLarge)
	require.Nil(t, result, "never return a truncated success")
}

func TestCommandProvenanceJobBounds(t *testing.T) {
	db := newCommandOriginDB(t)
	orgID := setCommandOriginScope(t, db, 77)
	batch := newCommandOriginBatch(t, db, uuid.New())
	projects := make([]string, MaxCommandProvenanceJobs+1)
	for i := range projects {
		projects[i] = fmt.Sprintf("project-%03d", i)
	}
	require.ErrorIs(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), projects), ErrProvenanceTooLarge)
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), projects[:MaxCommandProvenanceJobs]))
	// A transaction keeps this boundary test inexpensive while still exercising
	// the persisted production read and seal paths.
	require.NoError(t, db.GormDB.Transaction(func(txDB *gorm.DB) error {
		txdb := &Database{GormDB: txDB}
		for _, project := range projects[:MaxCommandProvenanceJobs] {
			newCommandOriginJob(t, txdb, batch, project)
		}
		return nil
	}))
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	result, err := db.GetCommandProvenance(orgID, "owner/repo", 41, 456)
	require.NoError(t, err)
	require.True(t, result.Batches[0].ManifestComplete)
	require.Len(t, result.Batches[0].Jobs, MaxCommandProvenanceJobs)
	newCommandOriginJob(t, db, batch, projects[MaxCommandProvenanceJobs])
	result, err = db.GetCommandProvenance(orgID, "owner/repo", 41, 456)
	require.ErrorIs(t, err, ErrProvenanceTooLarge)
	require.Nil(t, result)
}

func TestSealBatchCommandOriginJobBound(t *testing.T) {
	db := newCommandOriginDB(t)
	batch := newCommandOriginBatch(t, db, uuid.New())
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
	batchID := batch.ID.String()
	jobs := make([]DiggerJob, MaxCommandProvenanceJobs+1)
	for i := range jobs {
		jobs[i] = DiggerJob{BatchID: &batchID, DiggerJobID: fmt.Sprintf("job-%d", i)}
	}
	require.NoError(t, db.GormDB.CreateInBatches(&jobs, 100).Error)
	require.ErrorIs(t, db.SealBatchCommandOrigin(batch.ID), ErrProvenanceTooLarge)
	var origin BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin, "batch_id = ?", batch.ID).Error)
	require.False(t, origin.Sealed)
	require.Empty(t, origin.JobManifest)
}

func TestGetCommandProvenanceRejectsMissingScope(t *testing.T) {
	db := newCommandOriginDB(t)
	for _, query := range []struct {
		org     uint
		repo    string
		pr      int
		comment int64
	}{
		{0, "owner/repo", 41, 456}, {1, "", 41, 456},
		{1, "owner/repo", 0, 456}, {1, "owner/repo", 41, 0},
	} {
		got, err := db.GetCommandProvenance(query.org, query.repo, query.pr, query.comment)
		require.Error(t, err)
		require.Nil(t, got)
	}
}
