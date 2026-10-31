package services

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAmbiguousDispatchDoesNotAbandonReadySiblings(t *testing.T) {
	db, batch, child := schedulerDispatchFixture(t)
	second, err := db.CreateDiggerJob(batch.ID, child.SerializedJobSpec, child.WorkflowFile, nil, nil, "lazy", "second")
	require.NoError(t, err)
	batchID := batch.ID.String()
	parent := models.DiggerJob{DiggerJobID: "parent", BatchID: &batchID, Status: scheduler.DiggerJobSucceeded}
	require.NoError(t, db.GormDB.Create(&parent).Error)
	for _, c := range []*models.DiggerJob{child, second} {
		require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, c.DiggerJobID))
	}
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if posts.Add(1) == 1 {
			http.Error(w, "dispatch may be accepted; response unavailable", 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := github.NewClient(server.Client())
	client.BaseURL, _ = url.Parse(server.URL + "/")
	err = DiggerJobCompleted(client, &batch.ID, &parent, "owner/repo", "owner", "repo", "digger_workflow.yml", dispatchTokenProvider{})
	require.Error(t, err)
	// Even when the ambiguously accepted first child subsequently completes,
	// its completion callback does not revisit its parent's untouched sibling.
	var accepted models.DiggerJob
	require.NoError(t, db.GormDB.Preload("Batch").Where("batch_id=? AND workflow_dispatch_started_at IS NOT NULL", batchID).First(&accepted).Error)
	accepted.Status = scheduler.DiggerJobSucceeded
	require.NoError(t, db.UpdateDiggerJob(&accepted))
	require.NoError(t, DiggerJobCompleted(client, &batch.ID, &accepted, "owner/repo", "owner", "repo", "digger_workflow.yml", dispatchTokenProvider{}))
	var undispatched int64
	require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("batch_id=? AND id IN ? AND status=? AND workflow_dispatch_started_at IS NULL", batchID, []uint{child.ID, second.ID}, scheduler.DiggerJobCreated).Count(&undispatched).Error)
	firstPass := posts.Load()
	// An extra parent callback is not a durable retry, but proves the untouched
	// sibling was eligible and that its sibling's marker need not be released.
	require.NoError(t, DiggerJobCompleted(client, &batch.ID, &parent, "owner/repo", "owner", "repo", "digger_workflow.yml", dispatchTokenProvider{}))
	require.EqualValues(t, 2, posts.Load(), "the possible first dispatch must not repeat")
	require.Zero(t, undispatched, "one completed-parent pass must retain progress for every ready sibling")
	require.EqualValues(t, 2, firstPass, "both ready children should be attempted once")
}

func TestDispatchRechecksPersistedIdentityAfterPreparation(t *testing.T) {
	for _, change := range []string{"deleted", "job_id", "batch_id", "terminal"} {
		t.Run(change, func(t *testing.T) {
			db, batch, child := schedulerDispatchFixture(t)
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := github.NewClient(server.Client())
			client.BaseURL, _ = url.Parse(server.URL + "/")
			release := make(chan struct{})
			prepared := make(chan struct{}, 1)
			finished := make(chan error, 1)
			go func() {
				finished <- ScheduleJob(ci_backends.GithubActionCi{Client: client}, "owner/repo", "owner", "repo", &batch.ID, child, dispatchTokenProvider{prepared: prepared, release: release})
			}()
			select {
			case <-prepared:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("preparation did not reach token barrier")
			}
			switch change {
			case "deleted":
				require.NoError(t, db.GormDB.Delete(&models.DiggerJob{}, child.ID).Error)
			case "job_id":
				require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("digger_job_id", "changed-job").Error)
			case "batch_id":
				other := batch
				other.ID = uuid.New()
				require.NoError(t, db.GormDB.Create(&other).Error)
				require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("batch_id", other.ID.String()).Error)
			case "terminal":
				require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("status", scheduler.DiggerJobSucceeded).Error)
			}
			close(release)
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("dispatch did not finish")
			}
			require.Zero(t, posts.Load())
		})
	}
}
