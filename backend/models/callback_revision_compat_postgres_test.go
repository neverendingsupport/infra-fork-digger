package models

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJobCallbackPostgresPreviousScalarQualificationGuard(t *testing.T) {
	previousScalarQualificationGuard(t, false)
}

// The overlay replaces only UpdateDiggerJob with its exact nes-main body.
// The normal test above uses the frozen previous scalar schema instead.
func TestPreviousStorageHelperPostgresQualificationGuard(t *testing.T) {
	if os.Getenv("DIGGER_TEST_PREVIOUS_STORAGE_HELPER") != "1" {
		t.Skip("requires the previous-storage helper overlay")
	}
	previousScalarQualificationGuard(t, true)
}

func previousScalarQualificationGuard(t *testing.T, storageHelper bool) {
	t.Helper()
	for _, qualification := range []string{"none", "job", "sibling", "deleted_sibling"} {
		t.Run(qualification, func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			batch := newCommandOriginBatch(t, db, uuid.New())
			org := setCommandOriginScope(t, db, batch.GithubInstallationId)
			require.NoError(t, db.GormDB.Create(&Repo{Name: "compat", RepoFullName: batch.RepoFullName, OrganisationID: org}).Error)
			require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"parent", "child"}))
			makeJob := func(name string, qualified bool) *DiggerJob {
				t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", map[bool]string{false: "false", true: "true"}[qualified])
				body, err := json.Marshal(scheduler.JobJson{ProjectName: name, Commit: batch.CommitSha, JobType: string(batch.BatchType), Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber})
				require.NoError(t, err)
				check := "check-" + name
				job, err := db.CreateDiggerJob(batch.ID, body, "workflow.yml", &check, nil, "lazy", name)
				require.NoError(t, err)
				return job
			}
			parent := makeJob("parent", qualification == "job")
			child := makeJob("child", qualification == "sibling" || qualification == "deleted_sibling")
			require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
			require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
			if qualification == "deleted_sibling" {
				require.NoError(t, db.GormDB.Delete(child).Error)
			}
			t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
			started := *parent
			started.Status, started.StatusUpdatedAt = scheduler.DiggerJobStarted, time.Date(2026, 11, 12, 7, 0, 1, 0, time.UTC)
			if qualification != "none" {
				started.WorkflowRunID, started.WorkflowRunAttempt = 42665476673, 2
			}
			accepted, err := db.AcceptDiggerJobCallback(&started, org)
			require.NoError(t, err)
			// An old replica loads before the next accepted callback; a second
			// old replica loads after it. Neither knows callback_revision.
			var before dependencyPreviousJob
			require.NoError(t, db.GormDB.First(&before, parent.ID).Error)
			beforeCurrent := *accepted
			started.StatusUpdatedAt = started.StatusUpdatedAt.Add(time.Second)
			accepted, err = db.AcceptDiggerJobCallback(&started, org)
			require.NoError(t, err)
			require.Equal(t, int64(2), accepted.CallbackRevision)
			if !storageHelper {
				require.ErrorIs(t, db.UpdateDiggerJob(&beforeCurrent), ErrDiggerJobTransitionConflict, "current helper must keep its revision CAS")
			}
			for _, snapshot := range []string{"before", "after"} {
				previous := before
				current := beforeCurrent
				if snapshot == "after" {
					require.NoError(t, db.GormDB.First(&previous, parent.ID).Error)
					current = dependencySavedJob(t, db, parent.ID)
				}
				previous.Previous.Status, current.Status = scheduler.DiggerJobFailed, scheduler.DiggerJobFailed
				previous.Previous.StatusUpdatedAt = started.StatusUpdatedAt.Add(time.Second)
				current.StatusUpdatedAt = previous.Previous.StatusUpdatedAt
				if qualification != "none" {
					previous.WorkflowRunAttempt, current.WorkflowRunAttempt = 1, 1
				}
				if storageHelper {
					// Associations are irrelevant to this scalar compatibility test.
					current.Batch = nil
					err = db.UpdateDiggerJob(&current)
				} else {
					err = db.GormDB.Save(&previous).Error
				}
				if qualification == "none" {
					require.NoError(t, err, "unqualified previous scalar "+snapshot)
				} else {
					require.ErrorContains(t, err, "job callback revision cannot be bypassed", snapshot)
				}
			}
			saved := dependencySavedJob(t, db, parent.ID)
			require.Equal(t, int64(2), saved.CallbackRevision)
			if qualification == "none" {
				require.Equal(t, scheduler.DiggerJobFailed, saved.Status)
				require.Zero(t, saved.WorkflowRunID)
				require.NoError(t, db.UpdateBatchStatus(&batch))
				require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			} else {
				require.Equal(t, scheduler.DiggerJobStarted, saved.Status)
				require.Equal(t, int64(2), saved.WorkflowRunAttempt)
			}
			evidence, err := db.GetCommandProvenance(org, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
			require.NoError(t, err)
			require.Len(t, evidence.Batches, 1)
			require.Equal(t, qualification != "deleted_sibling", evidence.Batches[0].ManifestComplete)
			for _, job := range evidence.Batches[0].Jobs {
				require.Nil(t, job.ExecutionOutcome, "legacy writes must never create native proof")
			}
		})
	}
}

func TestDependencyRetirementPostgresUnqualifiedFenceRemainsUnconditional(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	db := commandOriginPostgresDB(t, true)
	batch := newCommandOriginBatch(t, db, uuid.New())
	job, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "child")
	require.NoError(t, err)
	require.Zero(t, job.DispatchProtocolVersion)
	// A synthetic legacy fence tests the unconditional storage invariant; this
	// is not a historical qualification operation or a never-dispatched proof.
	require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET dependency_retired_at = ?, dependency_failure_job_id = 'parent', status = 3 WHERE id = ?", time.Now().UTC(), job.ID).Error)
	var previous dependencyPreviousJob
	require.NoError(t, db.GormDB.First(&previous, job.ID).Error)
	previous.Previous.Status = scheduler.DiggerJobStarted
	require.ErrorContains(t, db.GormDB.Save(&previous).Error, "dependency-retired job cannot be reopened")
	require.ErrorContains(t, db.GormDB.Exec("UPDATE digger_jobs SET dependency_retired_at = NULL WHERE id = ?", job.ID).Error, "dependency-retired job cannot be reopened")
}
