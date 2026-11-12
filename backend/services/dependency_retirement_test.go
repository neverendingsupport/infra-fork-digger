package services

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestRetiredChildFencesPreparedSchedulerAndQueuedRetry(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "queued"}[queued], func(t *testing.T) {
			db, batch, child := schedulerDispatchFixture(t)
			batchID := batch.ID.String()
			parent := models.DiggerJob{DiggerJobID: "parent", BatchID: &batchID, Status: scheduler.DiggerJobSucceeded}
			require.NoError(t, db.GormDB.Create(&parent).Error)
			require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
			if queued {
				ok, err := db.QueueDiggerJobIfCreated(child)
				require.NoError(t, err)
				require.True(t, ok)
			}
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := github.NewClient(server.Client())
			client.BaseURL, _ = url.Parse(server.URL + "/")
			backend := ci_backends.GithubActionCi{Client: client}
			prepared, release := make(chan struct{}, 1), make(chan struct{})
			result := make(chan error, 1)
			go func() {
				result <- ScheduleJob(backend, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{prepared: prepared, release: release})
			}()
			select {
			case <-prepared:
			case <-time.After(5 * time.Second):
				close(release)
				t.Fatal("scheduler never reached pre-claim barrier")
			}
			parent.Status = scheduler.DiggerJobFailed
			parent.WorkflowRunID, parent.WorkflowRunAttempt = 42665476673, 1
			require.NoError(t, db.UpdateDiggerJob(&parent))
			repaired, err := db.ReconcileFailedDependencies(batch.ID)
			require.NoError(t, err)
			require.Equal(t, []string{child.DiggerJobID}, repaired.RetiredJobs)
			close(release)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("prepared scheduler did not return")
			}
			// A restart, duplicate stale successful-parent callback and cron retry
			// all use persisted permission, never their earlier in-memory status.
			models.DB = &models.Database{GormDB: db.GormDB.Session(&gorm.Session{NewDB: true})}
			parent.Status = scheduler.DiggerJobSucceeded
			require.NoError(t, DiggerJobCompleted(client, &batch.ID, &parent, "owner/repo", "owner", "repo", child.WorkflowFile, dispatchTokenProvider{}))
			require.NoError(t, ScheduleJob(backend, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{}))
			config.DiggerConfig.Set("max_concurrency_per_batch", 1)
			require.NoError(t, ScheduleJob(backend, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{}))
			require.Zero(t, posts.Load(), "no scheduling entrypoint may send retired work")
			saved, err := db.GetDiggerJob(child.DiggerJobID)
			require.NoError(t, err)
			require.NotNil(t, saved.DependencyRetiredAt)
			require.Nil(t, saved.WorkflowDispatchStartedAt)
		})
	}
}
