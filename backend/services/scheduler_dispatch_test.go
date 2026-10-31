package services

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type dispatchTokenProvider struct {
	utils.DiggerGithubClientMockProvider
	prepared chan struct{}
	release  <-chan struct{}
}

func (p dispatchTokenProvider) Get(_, _ int64) (*github.Client, *string, error) {
	if p.prepared != nil {
		p.prepared <- struct{}{}
		select {
		case <-p.release:
		case <-time.After(3 * time.Second):
			return nil, nil, errors.New("test token barrier timed out")
		}
	}
	token := "test-token"
	return github.NewClient(nil), &token, nil
}

func schedulerDispatchFixture(t *testing.T) (*models.Database, models.DiggerBatch, *models.DiggerJob) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "dispatch.db")+"?_busy_timeout=5000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.DiggerBatch{}, &models.DiggerJob{}, &models.DiggerJobParentLink{}, &models.GithubAppInstallation{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	previous, previousLimit := models.DB, config.DiggerConfig.GetInt("max_concurrency_per_batch")
	models.DB = &models.Database{GormDB: db}
	config.DiggerConfig.Set("max_concurrency_per_batch", 0)
	t.Cleanup(func() {
		models.DB = previous
		config.DiggerConfig.Set("max_concurrency_per_batch", previousLimit)
		require.NoError(t, sqlDB.Close())
	})
	batch := models.DiggerBatch{ID: uuid.New(), VCS: models.DiggerVCSGithub, PrNumber: 7, GithubInstallationId: 1, RepoFullName: "owner/repo", RepoOwner: "owner", RepoName: "repo", BatchType: scheduler.DiggerCommandPlan}
	require.NoError(t, db.Create(&batch).Error)
	require.NoError(t, db.Create(&models.GithubAppInstallation{GithubInstallationId: 1, GithubAppId: 2, Repo: "owner/repo", Status: models.GithubAppInstallActive}).Error)
	body, err := json.Marshal(scheduler.JobJson{ProjectName: "child", PullRequestNumber: &batch.PrNumber, RequestedBy: "operator", Branch: "main"})
	require.NoError(t, err)
	child, err := models.DB.CreateDiggerJob(batch.ID, body, "digger_workflow.yml", nil, nil, "lazy", "child")
	require.NoError(t, err)
	child.Batch = &batch
	return models.DB, batch, child
}

func TestConcurrentParentsDispatchSharedChildOnce(t *testing.T) {
	db, batch, child := schedulerDispatchFixture(t)
	batchID := batch.ID.String()
	parents := []models.DiggerJob{
		{DiggerJobID: "parent-a", BatchID: &batchID, Status: scheduler.DiggerJobSucceeded},
		{DiggerJobID: "parent-b", BatchID: &batchID, Status: scheduler.DiggerJobSucceeded},
	}
	for i := range parents {
		require.NoError(t, db.GormDB.Create(&parents[i]).Error)
		require.NoError(t, db.CreateDiggerJobParentLink(parents[i].DiggerJobID, child.DiggerJobID))
	}
	var posts atomic.Int64
	var unclaimed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/repos/owner/repo/actions/workflows/digger_workflow.yml/dispatches" {
			http.Error(w, "unexpected request", 400)
			return
		}
		posts.Add(1)
		saved, err := db.GetDiggerJob(child.DiggerJobID)
		if err != nil || saved.Status != scheduler.DiggerJobTriggered || saved.WorkflowDispatchStartedAt == nil {
			unclaimed.Store(true)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := github.NewClient(server.Client())
	client.BaseURL, _ = url.Parse(server.URL + "/")
	release := make(chan struct{})
	provider := dispatchTokenProvider{prepared: make(chan struct{}, 2), release: release}
	outcomes := make(chan error, 2)
	for i := range parents {
		go func(parent models.DiggerJob) {
			outcomes <- DiggerJobCompleted(client, &batch.ID, &parent, "owner/repo", "owner", "repo", "digger_workflow.yml", provider)
		}(parents[i])
	}
	for range parents {
		select {
		case <-provider.prepared:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("both completion handlers must reach dispatch preparation")
		}
	}
	close(release)
	for range parents {
		select {
		case err := <-outcomes:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("completion handler did not finish")
		}
	}
	require.EqualValues(t, 1, posts.Load())
	require.False(t, unclaimed.Load(), "dispatch must be claimed before the HTTP effect")
	require.NoError(t, DiggerJobCompleted(client, &batch.ID, &parents[0], "owner/repo", "owner", "repo", "digger_workflow.yml", dispatchTokenProvider{}))
	require.EqualValues(t, 1, posts.Load(), "a repeated parent callback cannot dispatch again")
}

func TestDispatchDoesNotOverwriteFastCompletionOrRetryLostResponse(t *testing.T) {
	for _, reply := range []string{"completed", "created", "lost"} {
		t.Run(reply, func(t *testing.T) {
			db, batch, child := schedulerDispatchFixture(t)
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				if reply == "lost" {
					http.Error(w, "accepted dispatch; response unavailable", 500)
					return
				}
				status := scheduler.DiggerJobSucceeded
				if reply == "created" {
					status = scheduler.DiggerJobCreated
				}
				if err := db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("status", status).Error; err != nil {
					http.Error(w, "test callback failed", 500)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"workflow_run_id":123,"run_url":"https://api.github.com/repos/owner/repo/actions/runs/123"}`))
			}))
			defer server.Close()
			client := github.NewClient(server.Client())
			client.BaseURL, _ = url.Parse(server.URL + "/")
			backend := ci_backends.GithubActionCi{Client: client}
			run := func() error {
				return ScheduleJob(backend, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{})
			}
			err := run()
			expected := scheduler.DiggerJobSucceeded
			if reply == "created" {
				expected = scheduler.DiggerJobCreated
			}
			if reply == "lost" {
				require.Error(t, err)
				expected = scheduler.DiggerJobTriggered
			} else {
				require.NoError(t, err)
			}
			saved, err := db.GetDiggerJob(child.DiggerJobID)
			require.NoError(t, err)
			require.Equal(t, expected, saved.Status)
			require.NotNil(t, saved.WorkflowDispatchStartedAt)
			require.NoError(t, run())
			require.EqualValues(t, 1, posts.Load())
		})
	}
}

func TestStaleConcurrencyWaitCannotRequeueTriggeredChild(t *testing.T) {
	db, batch, child := schedulerDispatchFixture(t)
	require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("status", scheduler.DiggerJobTriggered).Error)
	config.DiggerConfig.Set("max_concurrency_per_batch", 1)
	require.NoError(t, ScheduleJob(nil, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{}))
	saved, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, scheduler.DiggerJobTriggered, saved.Status)
}

func TestCreatedCallbackStillOccupiesBatchCapacity(t *testing.T) {
	db, batch, child := schedulerDispatchFixture(t)
	claimed, err := db.ClaimDiggerJobDispatch(child)
	require.NoError(t, err)
	require.True(t, claimed)
	child.Status = scheduler.DiggerJobCreated
	require.NoError(t, db.UpdateDiggerJob(child))
	count, err := db.CountActiveDiggerJobDispatches(batch.ID.String())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	next, err := db.CreateDiggerJob(batch.ID, child.SerializedJobSpec, child.WorkflowFile, nil, nil, "lazy", "next")
	require.NoError(t, err)
	config.DiggerConfig.Set("max_concurrency_per_batch", 1)
	// A nil backend proves capacity prevents reaching workflow dispatch.
	require.NoError(t, ScheduleJob(nil, "owner/repo", "owner", "repo", &batch.ID, next, dispatchTokenProvider{}))
	saved, err := db.GetDiggerJob(next.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, scheduler.DiggerJobQueuedForRun, saved.Status)
}
