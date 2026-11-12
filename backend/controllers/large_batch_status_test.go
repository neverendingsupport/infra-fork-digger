package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryLargeBatchRegisteredStatus(t *testing.T) {
	for _, activeSibling := range []bool{false, true} {
		t.Run(fmt.Sprint(activeSibling), func(t *testing.T) {
			t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
			event := commandOriginEvent()
			event.Repo.FullName = github.String("owner/repo")
			db, org, repo := commandPipelineDB(t, event)
			batch, err := db.CreateDiggerBatch(models.DiggerVCSGithub, event.Installation.GetID(), "owner", "repo", repo.RepoFullName, 9, "", "feature", scheduler.DiggerCommandApply, nil, 0, "", false, true, nil, "head", nil, nil)
			require.NoError(t, err)
			token, err := db.CreateDiggerJobToken(org.ID)
			require.NoError(t, err)
			batchID := batch.ID.String()
			jobs := make([]models.DiggerJob, models.MaxCommandProvenanceJobs+1)
			for i := range jobs {
				name := fmt.Sprintf("project-%d", i)
				body, err := json.Marshal(scheduler.JobJson{ProjectName: name, JobType: "apply", Namespace: repo.RepoFullName, BackendJobToken: token.Value, PullRequestNumber: &batch.PrNumber})
				require.NoError(t, err)
				jobs[i] = models.DiggerJob{DiggerJobID: fmt.Sprintf("large-%d", i), BatchID: &batchID, ProjectName: name, SerializedJobSpec: body, Status: scheduler.DiggerJobSucceeded}
			}
			jobs[0].Status = scheduler.DiggerJobCreated
			if activeSibling {
				jobs[len(jobs)-1].Status = scheduler.DiggerJobStarted
			}
			require.NoError(t, db.GormDB.CreateInBatches(&jobs, 50).Error)
			// These automatic batches have no captured command. The bounded
			// operator/provenance path remains separate from runner admission.
			_, err = db.ReconcileFailedDependencies(batch.ID)
			require.ErrorIs(t, err, models.ErrProvenanceTooLarge)
			provider := utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: commandOriginTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"total_count":0,"workflow_runs":[]}`)), Request: r}, nil
			})}}
			router := gin.New()
			router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), middleware.AccessLevel(models.CliJobAccessType, models.AdminPolicyType), (DiggerController{GithubClientProvider: provider}).SetJobStatusForProject)
			report := func(job models.DiggerJob, status string, second int) {
				payload := fmt.Sprintf(`{"status":%q,"timestamp":"2026-11-12T06:00:%02dZ"}`, status, second)
				req := httptest.NewRequest(http.MethodPost, "/repos/test/projects/test/jobs/"+job.DiggerJobID+"/set-status", strings.NewReader(payload))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token.Value)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				require.Zero(t, background.Wait(ctx))
				cancel()
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			}
			report(jobs[0], "started", 1)
			terminal := "succeeded"
			if activeSibling {
				terminal = "failed"
			}
			report(jobs[0], terminal, 2)
			current, err := db.GetDiggerBatch(&batch.ID)
			require.NoError(t, err)
			if activeSibling {
				require.NotEqual(t, scheduler.BatchJobFailed, current.Status)
				report(jobs[len(jobs)-1], "succeeded", 3)
				current, err = db.GetDiggerBatch(&batch.ID)
				require.NoError(t, err)
				require.Equal(t, scheduler.BatchJobFailed, current.Status)
			} else {
				require.Equal(t, scheduler.BatchJobSucceeded, current.Status)
			}
			var retired int64
			require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("batch_id = ? AND dependency_retired_at IS NOT NULL", batchID).Count(&retired).Error)
			require.Zero(t, retired)
		})
	}
}
