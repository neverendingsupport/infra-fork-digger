package controllers

import (
	"context"
	"encoding/json"
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
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestBitbucketProducerBindingAndRegisteredCallback(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	event := commandOriginEvent()
	event.Repo.FullName = github.String("owner/repo")
	db, org, placeholder := commandPipelineDB(t, event)
	require.NoError(t, db.GormDB.Unscoped().Delete(&placeholder).Error)
	project := configuration.Project{Name: "dev", WorkflowFile: "workflow.yml"}
	graph, err := configuration.CreateProjectDependencyGraph([]configuration.Project{project})
	require.NoError(t, err)
	pr := 9
	_, jobs, err := utils.ConvertJobsToDiggerJobs(scheduler.DiggerCommandPlan, "lazy", models.DiggerVCSBitbucket, org.ID,
		map[string]scheduler.Job{"dev": {ProjectName: "dev", Namespace: event.Repo.GetFullName(), PullRequestNumber: &pr}}, map[string]configuration.Project{"dev": project}, graph,
		0, "feature", pr, "owner", "repo", event.Repo.GetFullName(), "head", nil, "", 0, "", false, true, nil, nil, nil, nil)
	require.NoError(t, err)
	var owned models.Repo
	require.NoError(t, db.GormDB.Where("organisation_id = ? AND repo_full_name = ?", org.ID, event.Repo.GetFullName()).First(&owned).Error)
	require.Equal(t, models.DiggerVCSBitbucket, owned.VCS)
	var jobSpec scheduler.JobJson
	require.NoError(t, json.Unmarshal(jobs["dev"].SerializedJobSpec, &jobSpec))
	otherOrg := models.Organisation{Name: "outsider", ExternalId: "outsider", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&otherOrg).Error)
	foreign, err := db.CreateDiggerJobToken(otherOrg.ID)
	require.NoError(t, err)
	router := gin.New()
	router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), middleware.AccessLevel(models.CliJobAccessType, models.AdminPolicyType), (DiggerController{}).SetJobStatusForProject)
	report := func(token string) int {
		payload, _ := json.Marshal(map[string]interface{}{"status": "started", "timestamp": time.Now().UTC(), "workflow_url": "https://bitbucket.fixture/pipelines/1"})
		req := httptest.NewRequest(http.MethodPost, "/repos/test/projects/dev/jobs/"+jobs["dev"].DiggerJobID+"/set-status", strings.NewReader(string(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.Zero(t, background.Wait(ctx))
		return w.Code
	}
	require.Equal(t, http.StatusForbidden, report(foreign.Value))
	saved, err := db.GetDiggerJob(jobs["dev"].DiggerJobID)
	require.NoError(t, err)
	require.Nil(t, saved.WorkflowDispatchStartedAt)
	require.Equal(t, http.StatusOK, report(jobSpec.BackendJobToken))
}
