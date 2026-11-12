package main

import (
	"encoding/json"
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

func TestAstraRunQueueRetiredWorkSettles(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	for _, initial := range []models.DiggerRunStatus{models.RunQueued, models.RunApproved} {
		for _, state := range []string{"retired"} {
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
				RunQueuesStateMachine(&item, service, provider)
				// Retry the stale queue item as another worker would after restart.
				RunQueuesStateMachine(&item, service, provider)
				expected := int64(1)
				if state == "retired" {
					expected = 0
				}
				require.Equal(t, expected, posts.Load())
				saved, err := db.GetDiggerJob(child.DiggerJobID)
				require.NoError(t, err)
				if state == "retired" {
					require.NotNil(t, saved.DependencyRetiredAt)
					require.Nil(t, saved.WorkflowDispatchStartedAt)
					var savedRun models.DiggerRun
					require.NoError(t, gdb.First(&savedRun, run.ID).Error)
					var queued int64
					require.NoError(t, gdb.Model(&models.DiggerRunQueueItem{}).Where("digger_run_id = ?", run.ID).Count(&queued).Error)
					t.Logf("retired job: run status=%q queue rows=%d dispatches=%d", savedRun.Status, queued, posts.Load())
					if savedRun.Status != models.RunFailed || queued != 0 {
						t.Error("permanently retired work still holds the project queue")
					}

				} else {
					require.NotNil(t, saved.WorkflowDispatchStartedAt)
				}
			})
		}
	}
}
