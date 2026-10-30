package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type commandOriginTransport func(*http.Request) (*http.Response, error)

func (f commandOriginTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type commandOriginCI struct {
	orgID    uint
	event    *github.IssueCommentEvent
	runs     []spec.Spec
	projects int
	attempts int
}

func (c *commandOriginCI) GetCiBackend(ci_backends.CiBackendOptions) (ci_backends.CiBackend, error) {
	return c, nil
}

func (c *commandOriginCI) GetWorkflowUrl(spec.Spec) (string, error) { return "", nil }

func (c *commandOriginCI) TriggerWorkflow(job spec.Spec, _ string, _ string) error {
	c.attempts++
	// Inspect persisted production state at the first remote-effect boundary.
	// A report comment, a helper-only parser result or a later seal is insufficient.
	got, err := models.DB.GetCommandProvenance(c.orgID, c.event.Repo.GetFullName(), c.event.Issue.GetNumber(), c.event.Comment.GetID())
	if err != nil {
		return err
	}
	if len(got.Batches) != 1 || !got.Batches[0].ManifestComplete || len(got.Batches[0].Jobs) != c.projects {
		return fmt.Errorf("workflow dispatched without complete original-command manifest")
	}
	c.runs = append(c.runs, job)
	return nil
}

func commandPipelineDB(t *testing.T, event *github.IssueCommentEvent) (*models.Database, models.Organisation, models.Repo) {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "pipeline.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gdb.AutoMigrate(&models.Organisation{}, &models.Repo{}, &models.GithubAppInstallation{},
		&models.GithubAppInstallationLink{}, &models.DiggerBatch{}, &models.DiggerJob{}, &models.BatchCommandOrigin{},
		&models.GithubDiggerJobLink{}, &models.DiggerJobParentLink{}, &models.JobToken{}, &models.DetectionRun{}))
	// The two legacy ORM models reuse idx_org_repo. Preserve the Repo
	// uniqueness constraint under a test-only name before migrating the other.
	require.NoError(t, gdb.Exec("CREATE UNIQUE INDEX pipeline_repo_identity ON repos(name, organisation_id)").Error)
	require.NoError(t, gdb.Migrator().DropIndex(&models.Repo{}, "idx_org_repo"))
	require.NoError(t, gdb.AutoMigrate(&models.ImpactedProject{}))
	previous := models.DB
	db := &models.Database{GormDB: gdb}
	models.DB = db
	t.Cleanup(func() { models.DB = previous; sqlDB, _ := gdb.DB(); _ = sqlDB.Close() })
	org := models.Organisation{Name: "pipeline", ExternalId: "pipeline-id", ExternalSource: "test"}
	require.NoError(t, gdb.Create(&org).Error)
	repo := models.Repo{Name: "pipeline", RepoFullName: event.Repo.GetFullName(), OrganisationID: org.ID}
	require.NoError(t, gdb.Create(&repo).Error)
	_, err = db.CreateGithubInstallationLink(&org, event.Installation.GetID())
	require.NoError(t, err)
	_, err = db.CreateGithubAppInstallation(event.Installation.GetID(), 1, "owner", 1, event.Repo.GetFullName())
	require.NoError(t, err)
	return db, org, repo
}

func commandPipelineCheckout(t *testing.T, config string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "digger.yml"), []byte(config), 0600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dev"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dev", "main.tf"), []byte("# fixture\n"), 0600))
	for _, args := range [][]string{{"init", "-b", "feature"}, {"add", "."}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	// Production cloning still runs, against a local fixture. No network request
	// or credential is sent to the placeholder host.
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "url.file://"+dir+".insteadOf")
	t.Setenv("GIT_CONFIG_VALUE_0", "https://x-access-token:token@fixture.invalid/repo.git")
	return "https://fixture.invalid/repo.git"
}

func TestCommandOriginSignedWebhookToSealedDispatchAndAPI(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		projects     int
	}{
		{"one_project", "pr_locks: false\nprojects:\n- name: dev\n  dir: dev\n  workflow: default\n", 1},
		{"unequal_dependency_depth", "pr_locks: false\nprojects:\n- name: a\n  dir: dev\n  workflow: default\n- name: b\n  dir: dev\n  workflow: default\n  depends_on: [a]\n- name: d\n  dir: dev\n  workflow: default\n  depends_on: [b]\n- name: c\n  dir: dev\n  workflow: default\n  depends_on: [a, d]\n", 4},
	} {
		t.Run(tc.name, func(t *testing.T) { testCommandOriginPipeline(t, tc.config, tc.projects, "") })
	}
}

func TestCommandOriginPersistenceFailureNeverDispatches(t *testing.T) {
	config := "pr_locks: false\nprojects:\n- name: a\n  dir: dev\n  workflow: default\n- name: b\n  dir: dev\n  workflow: default\n  depends_on: [a]\n"
	for _, stage := range []string{"origin", "job", "job_link", "parent_link", "seal"} {
		t.Run(stage, func(t *testing.T) { testCommandOriginPipeline(t, config, 2, stage) })
	}
}

func testCommandOriginPipeline(t *testing.T, config string, projects int, failureStage string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("SEGMENT_API_KEY", "")
	t.Setenv("DIGGER_CONFIG_REPO_CACHE_ENABLED", "0")
	t.Setenv("DIGGER_INTERNAL_SECRET", "test-internal-secret")
	var event github.IssueCommentEvent
	require.NoError(t, json.Unmarshal([]byte(issueCommentPayload), &event))
	event.Repo.CloneURL = github.String(commandPipelineCheckout(t, config))
	event.Comment.Body = github.String("digger plan\n")
	db, org, repo := commandPipelineDB(t, &event)
	failureInjected := false
	jobCreates := 0
	injectCreateFailure := func(tx *gorm.DB) {
		table := tx.Statement.Table
		if table == "digger_jobs" {
			jobCreates++
		}
		fail := (failureStage == "origin" && table == "batch_command_origins") ||
			(failureStage == "job" && table == "digger_jobs" && jobCreates == 2) ||
			(failureStage == "job_link" && table == "github_digger_job_links") ||
			(failureStage == "parent_link" && table == "digger_job_parent_links")
		if fail {
			failureInjected = true
			tx.AddError(fmt.Errorf("injected %s failure", failureStage))
		}
	}
	require.NoError(t, db.GormDB.Callback().Create().Before("gorm:create").Register("test:command_origin_failure", injectCreateFailure))
	require.NoError(t, db.GormDB.Callback().Update().Before("gorm:update").Register("test:command_seal_failure", func(tx *gorm.DB) {
		if failureStage == "seal" && tx.Statement.Table == "batch_command_origins" {
			failureInjected = true
			tx.AddError(fmt.Errorf("injected seal failure"))
		}
	}))
	checkID := int64(1000)
	transport := commandOriginTransport(func(r *http.Request) (*http.Response, error) {
		status, body := http.StatusOK, "{}"
		switch {
		case strings.HasSuffix(r.URL.Path, "/pulls/2"):
			body = `{"number":2,"head":{"ref":"feature","sha":"head-sha"},"base":{"ref":"main","sha":"base-sha"},"merged":false}`
		case strings.HasSuffix(r.URL.Path, "/pulls/2/files"):
			body = `[{"filename":"dev/main.tf"}]`
		case strings.HasSuffix(r.URL.Path, "/pulls/2/merge"):
			status = http.StatusNotFound
		case strings.HasSuffix(r.URL.Path, "/contents/digger.yml"):
			body = fmt.Sprintf(`{"type":"file","encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString([]byte(config)))
		case strings.HasSuffix(r.URL.Path, "/reviews"), strings.HasSuffix(r.URL.Path, "/teams"):
			body = "[]"
		case strings.Contains(r.URL.Path, "/issues/") && strings.HasSuffix(r.URL.Path, "/comments"):
			body = `{"id":9000,"html_url":"https://github.com/owner/repo/issues/2#issuecomment-9000"}`
		case strings.Contains(r.URL.Path, "/issues/comments/"):
			body = `{"id":9000}`
		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			checkID++
			body = fmt.Sprintf(`{"id":%d}`, checkID)
		case strings.Contains(r.URL.Path, "/check-runs/"):
			body = `{"id":1001,"name":"dev/plan","status":"in_progress","output":{"title":"Plan","summary":"Running"}}`
		default:
			return nil, fmt.Errorf("unexpected GitHub request: %s %s", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	provider := webhookCredentialsProvider{DiggerGithubClientMockProvider: utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: transport}}, webhookSecret: "test-webhook-secret"}
	ci := &commandOriginCI{orgID: org.ID, event: &event, projects: projects}
	controller := DiggerController{GithubClientProvider: provider, CiBackendProvider: ci}
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	for _, signed := range []bool{false, true} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/github/webhook", strings.NewReader(string(payload)))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request.Header.Set("X-GitHub-Event", "issue_comment")
		c.Request.Header.Set("X-GitHub-Hook-Installation-Target-ID", "1")
		if signed {
			c.Request.Header.Set("X-Hub-Signature-256", signGithubPayload(provider.webhookSecret, string(payload)))
		}
		controller.GithubAppWebHook(c)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.Zero(t, background.Wait(ctx))
		cancel()
		if !signed {
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Empty(t, ci.runs)
			require.Zero(t, ci.attempts)
		} else {
			require.Equal(t, http.StatusAccepted, w.Code)
			if failureStage != "" {
				require.True(t, failureInjected, "test must reach the intended persistence transition")
				require.Empty(t, ci.runs, "partial persistence must not dispatch any workflow")
				require.Zero(t, ci.attempts, "partial persistence must not even attempt dispatch")
				var origins []models.BatchCommandOrigin
				require.NoError(t, db.GormDB.Find(&origins).Error)
				if failureStage == "origin" {
					require.Empty(t, origins)
				} else {
					require.Len(t, origins, 1)
					require.False(t, origins[0].Sealed)
					require.Empty(t, origins[0].JobManifest)
				}
				return
			}
			require.Len(t, ci.runs, 1)
			require.Equal(t, 1, ci.attempts)
		}
	}
	var origin models.BatchCommandOrigin
	require.NoError(t, db.GormDB.First(&origin).Error)
	require.True(t, origin.Sealed)
	require.Equal(t, event.Comment.GetID(), origin.SourceCommentID)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(event.Comment.GetBody()))), origin.BodySHA256)
	require.Equal(t, "head-sha", origin.CommitSHA)
	require.NotEqual(t, int64(9000), origin.SourceCommentID)

	router := gin.New()
	router.GET("/api/repos/:repo_id/jobs", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), GetJobsForRepoApi)
	query := fmt.Sprintf("/api/repos/%d/jobs?source_comment_id=%d&pr_number=2", repo.ID, event.Comment.GetID())
	for _, authenticated := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodGet, query, nil)
		req.Header.Set("DIGGER_ORG_ID", org.ExternalId)
		req.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
		req.Header.Set("DIGGER_USER_ID", "reader")
		if authenticated {
			req.Header.Set("Authorization", "Bearer test-internal-secret")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if !authenticated {
			require.Equal(t, http.StatusForbidden, w.Code)
			continue
		}
		require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
		var response struct {
			Provenance models.CommandProvenance `json:"command_provenance"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
		require.True(t, response.Provenance.Complete)
		require.Len(t, response.Provenance.Batches, 1)
		require.True(t, response.Provenance.Batches[0].ManifestComplete)
		require.Len(t, response.Provenance.Batches[0].Jobs, projects)
		require.Equal(t, ci.runs[0].JobId, response.Provenance.Batches[0].Jobs[0].JobID)
		require.NotContains(t, w.Body.String(), "backend_job_token")
	}
	for _, suffix := range []string{
		"?source_comment_id=1", "?source_comment_id=1&pr_number=0",
		"?source_comment_id=1&source_comment_id=2&pr_number=2",
		"?source_comment_id=1&pr_number=2&pr_number=3", "?source_comment_id=invalid&pr_number=2",
	} {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/repos/%d/jobs%s", repo.ID, suffix), nil)
		req.Header.Set("Authorization", "Bearer test-internal-secret")
		req.Header.Set("DIGGER_ORG_ID", org.ExternalId)
		req.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
		req.Header.Set("DIGGER_USER_ID", "reader")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, suffix)
	}

	// Exercise the CLI status boundary with the real job-token middleware.
	// A token for another job must not overwrite attempt evidence.
	router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), controller.SetJobStatusForProject)
	otherToken, err := db.CreateDiggerJobToken(org.ID)
	require.NoError(t, err)
	job := ci.runs[0]
	workflow := "https://github.com/" + event.Repo.GetFullName() + "/actions/runs/404"
	for _, tc := range []struct {
		name, token, url string
		run, attempt     int64
		wantStatus       int
	}{
		{"other job", otherToken.Value, workflow, 404, 2, http.StatusForbidden},
		{"wrong run", job.Job.BackendJobToken, workflow, 405, 2, http.StatusBadRequest},
		{"current attempt", job.Job.BackendJobToken, workflow, 404, 2, http.StatusOK},
		{"legacy report", job.Job.BackendJobToken, workflow, 0, 0, http.StatusOK},
	} {
		body, err := json.Marshal(SetJobStatusRequest{Status: "started", WorkflowUrl: tc.url, WorkflowRunID: tc.run, WorkflowRunAttempt: tc.attempt})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/repos/test/projects/dev/jobs/"+job.JobId+"/set-status", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.Zero(t, background.Wait(ctx))
		cancel()
		require.Equal(t, tc.wantStatus, w.Code, "%s: %s", tc.name, w.Body.String())
		stored, err := db.GetDiggerJob(job.JobId)
		require.NoError(t, err)
		if tc.wantStatus == http.StatusOK {
			require.Equal(t, tc.run, stored.WorkflowRunID)
			require.Equal(t, tc.attempt, stored.WorkflowRunAttempt)
			require.Equal(t, tc.url, *stored.WorkflowRunUrl)
		} else {
			require.Zero(t, stored.WorkflowRunID)
			require.Zero(t, stored.WorkflowRunAttempt)
		}
	}
}
