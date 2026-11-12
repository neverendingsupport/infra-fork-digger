package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	github2 "github.com/diggerhq/digger/libs/ci/github"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestRunQueueUsesPersistentDispatchFence(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	for _, initial := range []models.DiggerRunStatus{models.RunQueued, models.RunApproved} {
		for _, state := range []string{"ready", "retired", "ambiguous", "failed"} {
			t.Run(string(initial)+"/"+state, func(t *testing.T) {
				gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "queue.db")), &gorm.Config{})
				require.NoError(t, err)
				require.NoError(t, gdb.AutoMigrate(&models.DiggerBatch{}, &models.DiggerJob{}, &models.DiggerJobParentLink{}, &models.GithubDiggerJobLink{}, &models.GithubAppInstallation{}, &models.DiggerRun{}, &models.DiggerRunQueueItem{}))
				previous := models.DB
				models.DB = &models.Database{GormDB: gdb}
				t.Cleanup(func() { models.DB = previous; sqlDB, _ := gdb.DB(); _ = sqlDB.Close() })
				db := models.DB
				batch := models.DiggerBatch{ID: uuid.New(), VCS: models.DiggerVCSGithub, PrNumber: 7, RepoFullName: "owner/repo", RepoOwner: "owner", RepoName: "repo", GithubInstallationId: 1, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated}
				require.NoError(t, gdb.Create(&batch).Error)
				require.NoError(t, gdb.Create(&models.GithubAppInstallation{GithubAppId: 2, GithubInstallationId: 1, Repo: batch.RepoFullName, Status: models.GithubAppInstallActive}).Error)
				body, err := json.Marshal(scheduler.JobJson{ProjectName: "child", PullRequestNumber: &batch.PrNumber})
				require.NoError(t, err)
				child, err := db.CreateDiggerJob(batch.ID, body, "workflow.yml", nil, nil, "lazy", "child")
				require.NoError(t, err)
				if state == "retired" {
					batchID := batch.ID.String()
					parent := models.DiggerJob{DiggerJobID: "parent", BatchID: &batchID, Status: scheduler.DiggerJobFailed, WorkflowRunID: 42665476673, WorkflowRunAttempt: 1}
					require.NoError(t, gdb.Create(&parent).Error)
					require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
					result, err := db.ReconcileFailedDependencies(batch.ID)
					require.NoError(t, err)
					require.Equal(t, []string{child.DiggerJobID}, result.RetiredJobs)
				}
				if state == "failed" {
					child.Status = scheduler.DiggerJobFailed
					child.WorkflowRunID, child.WorkflowRunAttempt = 99, 1
					require.NoError(t, db.UpdateDiggerJob(child))
				}
				batchID := batch.ID.String()
				stage := models.DiggerRunStage{BatchID: &batchID}
				run := models.DiggerRun{Status: initial, PlanStage: stage, ApplyStage: stage}
				require.NoError(t, gdb.Create(&run).Error)
				item := models.DiggerRunQueueItem{DiggerRunId: run.ID, DiggerRun: run}
				require.NoError(t, gdb.Create(&item).Error)
				var posts atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					posts.Add(1)
					if state == "ambiguous" {
						http.Error(w, "dispatch accepted; response lost", 500)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer server.Close()
				client := github.NewClient(server.Client())
				client.BaseURL, _ = url.Parse(server.URL + "/")
				service := &github2.GithubService{Client: client, Owner: "owner", RepoName: "repo"}
				provider := utils.DiggerGithubClientMockProvider{MockedHTTPClient: server.Client()}
				if state == "retired" {
					require.NoError(t, gdb.Callback().Delete().Before("gorm:delete").Register("test:dequeue_failure", func(tx *gorm.DB) { tx.AddError(fmt.Errorf("injected dequeue failure")) }))
					RunQueuesStateMachine(&item, service, provider)
					var unchanged models.DiggerRun
					require.NoError(t, gdb.First(&unchanged, run.ID).Error)
					require.Equal(t, initial, unchanged.Status, "dequeue failure must roll back run failure")
					require.NoError(t, gdb.Callback().Delete().Remove("test:dequeue_failure"))
				}
				RunQueuesStateMachine(&item, service, provider)
				// Retry the stale queue item as another worker would after restart.
				RunQueuesStateMachine(&item, service, provider)
				expected := int64(1)
				if state == "retired" || state == "failed" {
					expected = 0
				}
				require.Equal(t, expected, posts.Load())
				saved, err := db.GetDiggerJob(child.DiggerJobID)
				require.NoError(t, err)
				if state == "retired" {
					require.NotNil(t, saved.DependencyRetiredAt)
					require.Nil(t, saved.WorkflowDispatchStartedAt)
					var failed models.DiggerRun
					require.NoError(t, gdb.First(&failed, run.ID).Error)
					require.Equal(t, models.RunFailed, failed.Status)
					var retained int64
					require.NoError(t, gdb.Model(&models.DiggerRunQueueItem{}).Where("id = ?", item.ID).Count(&retained).Error)
					require.Zero(t, retained)
					// A later item for this same project becomes the actual head.
					nextBatch := batch
					nextBatch.ID = uuid.New()
					require.NoError(t, gdb.Create(&nextBatch).Error)
					_, err := db.CreateDiggerJob(nextBatch.ID, body, "workflow.yml", nil, nil, "lazy", "next")
					require.NoError(t, err)
					nextID := nextBatch.ID.String()
					nextStage := models.DiggerRunStage{BatchID: &nextID}
					nextRun := models.DiggerRun{Status: initial, PlanStage: nextStage, ApplyStage: nextStage}
					require.NoError(t, gdb.Create(&nextRun).Error)
					nextItem := models.DiggerRunQueueItem{DiggerRunId: nextRun.ID, DiggerRun: nextRun}
					require.NoError(t, gdb.Create(&nextItem).Error)
					heads, err := db.GetFirstRunQueueForEveryProject()
					require.NoError(t, err)
					require.Len(t, heads, 1)
					require.Equal(t, nextItem.ID, heads[0].ID)
					RunQueuesStateMachine(&heads[0], service, provider)
					require.Equal(t, int64(1), posts.Load(), "next item dispatches after retired head settles")

				} else if state == "failed" {
					var terminal models.DiggerRun
					require.NoError(t, gdb.First(&terminal, run.ID).Error)
					require.Equal(t, models.RunFailed, terminal.Status)
				} else {
					require.NotNil(t, saved.WorkflowDispatchStartedAt)
					if state == "ambiguous" {
						var pending models.DiggerRun
						require.NoError(t, gdb.First(&pending, run.ID).Error)
						require.Equal(t, initial, pending.Status, "a claim is not provider completion")
					}
				}
			})
		}
	}
}
