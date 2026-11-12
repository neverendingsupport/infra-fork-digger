package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestLegacyStopRegisteredOperatorAndProvenanceAPI(t *testing.T) { legacyStopRegistered(t, "") }
func TestLegacyStopRegisteredPublicationAdmissionRace(t *testing.T) {
	legacyStopRegistered(t, "publication_admission_race")
}
func TestLegacyStopRegisteredCheckpointReruns(t *testing.T) {
	for _, name := range []string{"before_verify_failed_rerun", "before_verify_successful_rerun", "after_verify_failed_rerun", "after_verify_successful_rerun"} {
		t.Run(name, func(t *testing.T) { legacyStopRegistered(t, name) })
	}
}
func legacyStopRegistered(t *testing.T, scenario string) {
	gin.SetMode(gin.TestMode)
	t.Setenv("DIGGER_INTERNAL_SECRET", "legacy-internal-test")
	t.Setenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED", "false")
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	event := commandOriginEvent()
	event.Repo.ID = github.Int64(954216504)
	event.Repo.FullName = github.String("neverendingsupport/infra-terraform")
	event.Installation = &github.Installation{ID: github.Int64(171074811)}
	event.Comment.ID = github.Int64(6746866728)
	event.Comment.Body = github.String("digger apply")
	db, org, repo := commandPipelineDB(t, event)
	require.NoError(t, db.GormDB.AutoMigrate(&models.DependencyStop{}, &models.DependencyStopCheckpoint{}))
	check, report := "129327714611", int64(6746869800)
	batch := models.DiggerBatch{ID: uuid.MustParse("4bddc990-19fe-4838-a267-b072df73aa82"), VCS: models.DiggerVCSGithub, RepoFullName: event.Repo.GetFullName(), RepoOwner: "neverendingsupport", RepoName: "infra-terraform", PrNumber: 3197, CommitSha: "ecdc93f734c02e56cd6e0fbe6b324a6a0600b07a", GithubInstallationId: 171074811, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated, CheckRunId: &check, CommentId: &report, DiggerConfig: "projects:\n  - name: access-control\n    dir: access-control\n  - name: infra-k8s\n    dir: infra-k8s\n    depends_on: [access-control]\n"}
	require.NoError(t, db.GormDB.Create(&batch).Error)
	source, err := githubCommandSource(event)
	require.NoError(t, err)
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, *source, []string{"access-control", "infra-k8s"}))
	makeJob := func(id uint, jobID, project, check string, status scheduler.DiggerJobStatus, runID int64) (models.DiggerJob, string) {
		token, err := db.CreateDiggerJobToken(org.ID)
		require.NoError(t, err)
		spec, err := json.Marshal(scheduler.JobJson{ProjectName: project, JobType: "apply", Commands: []string{"digger apply"}, Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber, Commit: batch.CommitSha, BackendJobToken: token.Value})
		require.NoError(t, err)
		summary := models.DiggerJobSummary{}
		require.NoError(t, db.GormDB.Create(&summary).Error)
		batchID, url := batch.ID.String(), "#"
		attempt := int64(0)
		if runID > 0 {
			attempt = 1
			url = "https://github.com/neverendingsupport/infra-terraform/actions/runs/42665476673"
		}
		checkURL := "https://github.com/neverendingsupport/infra-terraform/runs/" + check
		job := models.DiggerJob{Model: gorm.Model{ID: id}, DiggerJobID: jobID, ProjectName: project, BatchID: &batchID, CheckRunId: &check, CheckRunUrl: &checkURL, SerializedJobSpec: spec, Status: status, WorkflowRunID: runID, WorkflowRunAttempt: attempt, WorkflowRunUrl: &url, DiggerJobSummaryID: summary.ID}
		require.NoError(t, db.GormDB.Create(&job).Error)
		return job, token.Value
	}
	parent, parentToken := makeJob(2752, "b2pIz8ZHrR", "access-control", "129327710180", scheduler.DiggerJobFailed, 42665476673)
	require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET workflow_dispatch_started_at=? WHERE id=?", time.Date(2026, 11, 9, 17, 31, 38, 101616000, time.UTC), parent.ID).Error)
	child, childToken := makeJob(2753, "wWyMzTVZcl", "infra-k8s", "129327703323", scheduler.DiggerJobCreated, 0)
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	_, err = db.CreateDiggerJobLink(child.DiggerJobID, batch.RepoFullName)
	require.NoError(t, err)
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	active, failReports := true, false
	providerWrites := 0
	latestAttempt := 1
	latestConclusion := "failure"
	wrongReports := []string{}
	driftOnCheck := false
	reportBodies := []string{}
	patches := map[string]map[string]interface{}{}
	var providerMu sync.Mutex
	transport := commandOriginTransport(func(r *http.Request) (*http.Response, error) {
		providerMu.Lock()
		defer providerMu.Unlock()
		status, body := http.StatusOK, `{}`
		switch {
		case strings.HasSuffix(r.URL.Path, "/issues/comments/6746866728") && r.Method == "GET":
			body = `{"id":6746866728,"body":"digger apply","issue_url":"https://api.github.com/repos/neverendingsupport/infra-terraform/issues/3197"}`
		case strings.HasSuffix(r.URL.Path, "/pulls/3197"):
			body = fmt.Sprintf(`{"number":3197,"state":"closed","merged":true,"head":{"sha":%q},"merge_commit_sha":%q,"base":{"repo":{"id":954216504,"full_name":"neverendingsupport/infra-terraform"}}}`, strings.Repeat("1", 40), strings.Repeat("2", 40))
		case strings.HasSuffix(r.URL.Path, "/actions/runs/42665476673"):
			body = fmt.Sprintf(`{"id":42665476673,"run_attempt":%d,"status":"completed","conclusion":%q,"repository":{"id":954216504,"full_name":"neverendingsupport/infra-terraform"}}`, latestAttempt, latestConclusion)
		case strings.HasSuffix(r.URL.Path, "/actions/runs/42665476673/attempts/1"):
			body = `{"id":42665476673,"run_attempt":1,"status":"completed","conclusion":"failure","repository":{"id":954216504,"full_name":"neverendingsupport/infra-terraform"}}`
		case strings.HasSuffix(r.URL.Path, "/actions/runs"):
			body = `{"total_count":0,"workflow_runs":[]}`
			if active {
				body = `{"total_count":1,"workflow_runs":[{"id":999}]}`
			}
		case strings.Contains(r.URL.Path, "/check-runs/"):
			body = `{"id":129327714611,"name":"digger/apply","status":"in_progress","output":{"title":"Apply","summary":"Running"}}`
			if r.Method == "PATCH" {
				if driftOnCheck {
					driftOnCheck = false
					require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report+1).Error)
				}
				providerWrites++
				var payload map[string]interface{}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
				patches[r.URL.Path] = payload
				if failReports {
					status = http.StatusServiceUnavailable
				}
			}
		case strings.Contains(r.URL.Path, "/issues/comments/") && r.Method == "PATCH":
			if !strings.HasSuffix(r.URL.Path, "/issues/comments/6746869800") {
				wrongReports = append(wrongReports, r.URL.Path)
			}
			providerWrites++
			var payload struct {
				Body string `json:"body"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
			reportBodies = append(reportBodies, payload.Body)
			body = `{"id":6746869800}`
			if failReports {
				status = http.StatusServiceUnavailable
			}
		default:
			t.Fatalf("unexpected provider effect %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	controller := DiggerController{GithubClientProvider: utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: transport}}}
	router := gin.New()
	router.POST("/api/repos/:repo_id/batches/:batch_id/retire-failed-dependents", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), controller.StopLegacyDependentApi)
	router.POST("/api/repos/:repo_id/batches/:batch_id/reconcile-dependencies", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), controller.ReconcileBatchDependenciesApi)
	router.GET("/api/repos/:repo_id/jobs", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), GetJobsForRepoApi)
	router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), middleware.AccessLevel(models.CliJobAccessType, models.AccessPolicyType, models.AdminPolicyType), controller.SetJobStatusForProject)
	path := fmt.Sprintf("/api/repos/%d/batches/%s/retire-failed-dependents", repo.ID, batch.ID)
	req := models.DependencyStopRequest{Version: 1, Mode: "plan", OperationID: uuid.MustParse("a06c8142-4143-422b-9e44-377766aa3197"), PRNumber: 3197, SourceCommentID: source.CommentID, BodySHA256: source.BodySHA256, CommitSHA: batch.CommitSha, Targets: []models.DependencyStopJobRef{{DBID: child.ID, JobID: child.DiggerJobID}}, FailedPrerequisite: models.DependencyStopPrerequisite{DependencyStopJobRef: models.DependencyStopJobRef{DBID: parent.ID, JobID: parent.DiggerJobID}, WorkflowRunID: parent.WorkflowRunID, WorkflowRunAttempt: 1}, SettlementPolicy: models.DependencyStopPolicy}
	call := func(method, path, body, token, orgID string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("DIGGER_ORG_ID", orgID)
		r.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
		r.Header.Set("DIGGER_USER_ID", "operator")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.Zero(t, background.Wait(ctx))
		return w
	}
	saveReport := func(phase string) {
		require.NotEmpty(t, reportBodies)
		body := reportBodies[len(reportBodies)-1]
		require.NotContains(t, body, "href='#'>Apply", "ordinary Apply links must remain valid GitHub checks")
		if dir := os.Getenv("DIGGER_LEGACY_STOP_EXPORT_DIR"); dir != "" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "digger-legacy-report-"+phase+".md"), []byte(body), 0600))
		}
	}
	parentCallback := fmt.Sprintf(`{"status":"failed","timestamp":%q,"workflow_url":"https://github.com/neverendingsupport/infra-terraform/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":1}`, time.Now().UTC().Format(time.RFC3339Nano))
	initial := call("POST", "/repos/infra-terraform/projects/access-control/jobs/"+parent.DiggerJobID+"/set-status", parentCallback, parentToken, org.ExternalId)
	require.Equal(t, http.StatusOK, initial.Code, initial.Body.String())
	require.Contains(t, reportBodies[len(reportBodies)-1], "href='#'>created")
	saveReport("before_fence")
	initialWrites := providerWrites
	encoded := func() string { b, err := json.Marshal(req); require.NoError(t, err); return string(b) }
	require.Equal(t, http.StatusForbidden, call("POST", path, encoded(), "wrong-secret", org.ExternalId).Code)
	require.Equal(t, http.StatusForbidden, call("POST", path, encoded(), childToken, org.ExternalId).Code, "job JWT grants no operator API authority")
	require.Equal(t, http.StatusNotFound, call("POST", path, encoded(), "legacy-internal-test", "other-org").Code)
	foreign := models.Organisation{Name: "other", ExternalId: "existing-other-org", ExternalSource: org.ExternalSource}
	require.NoError(t, db.GormDB.Create(&foreign).Error)
	require.Equal(t, http.StatusNotFound, call("POST", path, encoded(), "legacy-internal-test", foreign.ExternalId).Code, "an existing foreign organisation cannot select this repository")
	require.Equal(t, http.StatusBadRequest, call("POST", path, strings.TrimSuffix(encoded(), "}")+`,"unexpected":true}`, "legacy-internal-test", org.ExternalId).Code)
	require.Equal(t, http.StatusBadRequest, call("POST", path, encoded()+"{}", "legacy-internal-test", org.ExternalId).Code)
	require.Equal(t, http.StatusRequestEntityTooLarge, call("POST", path, strings.TrimSuffix(encoded(), "}")+`,"unexpected":"`+strings.Repeat("x", 17000)+`"}`, "legacy-internal-test", org.ExternalId).Code)
	// A missing report ID holds the operation before any provider write.
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", nil).Error)
	require.Equal(t, http.StatusConflict, call("POST", path, encoded(), "legacy-internal-test", org.ExternalId).Code)
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report).Error)
	w := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, initialWrites, providerWrites)
	var planned struct {
		Plan models.DependencyStopPlan `json:"plan"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &planned))
	require.Equal(t, report, planned.Plan.Snapshot.ReportCommentID)
	req.Mode, req.ExpectedPlanSHA256 = "execute", planned.Plan.PlanSHA256

	if scenario == "publication_admission_race" {
		t.Setenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED", "true")
		injected := false
		require.NoError(t, db.GormDB.Callback().Query().After("gorm:query").Register("test:first_fence_after_count", func(tx *gorm.DB) {
			if injected || tx.Statement.Table != "digger_jobs" || !strings.Contains(tx.Statement.SQL.String(), "count(") || !strings.Contains(tx.Statement.SQL.String(), "dependency_retirement_operation_id IS NOT NULL") {
				return
			}
			injected = true
			response := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
			require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
			require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report+1).Error)
		}))
		t.Cleanup(func() { _ = db.GormDB.Callback().Query().Remove("test:first_fence_after_count") })
		err := utils.UpdatePRCommentRealtime(controller.GithubClientProvider, &batch)
		require.True(t, injected)
		require.ErrorIs(t, err, models.ErrDependencyStopConflict)
		require.Empty(t, wrongReports)
		return
	}
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	unfenced, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.Nil(t, unfenced.DependencyRetiredAt)
	t.Setenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED", "true")
	failReports = true
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "persisted")
	saved, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.True(t, saved.DependencyStopPending())
	fence := *saved.DependencyRetiredAt
	failReports = false
	t.Setenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED", "false") // existing fences can still verify after admission closes
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	saved, err = db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.True(t, fence.Equal(*saved.DependencyRetiredAt))
	proofPath := fmt.Sprintf("/api/repos/%d/jobs?source_comment_id=%d&pr_number=3197", repo.ID, source.CommentID)
	export := func(phase string, tags ...string) {
		tag := phase
		if scenario != "" {
			tag = scenario + "_initial_" + phase
		}
		if len(tags) > 0 {
			tag = tags[0]
		}
		w := call("GET", proofPath, "", "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"report_comment_id":6746869800`)
		require.Contains(t, w.Body.String(), `"prior_execution":"unknown"`)
		var payload struct {
			Provenance *models.CommandProvenance `json:"command_provenance"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
		require.NotNil(t, payload.Provenance)
		var out *models.ExecutionOutcome
		for _, job := range payload.Provenance.Batches[0].Jobs {
			if job.JobID == child.DiggerJobID {
				out = job.ExecutionOutcome
			}
		}
		require.NotNil(t, out)
		require.Equal(t, "future_dispatch_fenced", out.Kind)
		require.Equal(t, phase, out.SettlementState)
		saveReport(tag)
		if dir := os.Getenv("DIGGER_LEGACY_STOP_EXPORT_DIR"); dir != "" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "digger-legacy-provenance-"+tag+".json"), w.Body.Bytes(), 0600))
		}
	}
	export("fenced_pending_verification")
	// Retrying an existing operation may edit only its audited report identity.
	writesBefore := providerWrites
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report+1).Error)
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	require.Equal(t, writesBefore, providerWrites)

	t.Run("report_drift_callback", func(t *testing.T) {
		response := call("POST", "/repos/infra-terraform/projects/access-control/jobs/"+parent.DiggerJobID+"/set-status", parentCallback, parentToken, org.ExternalId)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.Empty(t, wrongReports)
	})
	t.Run("report_drift_native_reconcile", func(t *testing.T) {
		body := fmt.Sprintf(`{"pr_number":3197,"source_comment_id":%d,"commit_sha":%q}`, source.CommentID, batch.CommitSha)
		response := call("POST", strings.Replace(path, "retire-failed-dependents", "reconcile-dependencies", 1), body, "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
		require.Empty(t, wrongReports)
	})
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report).Error)
	t.Run("report_drift_during_check_patch", func(t *testing.T) {
		driftOnCheck = true
		response := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
		require.Empty(t, wrongReports)
	})
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("comment_id", report).Error)

	t.Run("report_and_check_destinations_use_audited_ids", func(t *testing.T) {
		before := providerWrites
		require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("check_run_id", "999999999").Error)
		response := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
		require.Equal(t, before, providerWrites)
		require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("check_run_id", *child.CheckRunId).Error)
		require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("check_run_id", "999999999").Error)
		response = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
		require.Equal(t, before, providerWrites)
		require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Where("id=?", batch.ID).Update("check_run_id", check).Error)
		require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("check_run_url", "https://github.com/other/repo/runs/999999999").Error)
		response = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
		require.NotContains(t, reportBodies[len(reportBodies)-1], "other/repo")
		require.Contains(t, reportBodies[len(reportBodies)-1], "infra-terraform/runs/129327703323")
		require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", child.ID).Update("check_run_url", *child.CheckRunUrl).Error)
	})
	require.Equal(t, "in_progress", patches["/repos/neverendingsupport/infra-terraform/check-runs/129327703323"]["status"])
	require.NotContains(t, strings.Join(reportBodies, "\n"), "never dispatched")
	late := `{"status":"started","timestamp":"2026-11-12T12:00:01Z","workflow_url":"https://github.com/neverendingsupport/infra-terraform/actions/runs/999","workflow_run_id":999,"workflow_run_attempt":1}`
	require.Equal(t, http.StatusConflict, call("POST", "/repos/infra-terraform/projects/infra-k8s/jobs/"+child.DiggerJobID+"/set-status", late, childToken, org.ExternalId).Code)
	// The optional subprocess binary uses the unchanged pinned RunSpec and can
	// be built with the deployed 2258d2 backend API overlay for qualification.
	t.Run("pinned_runner_against_registered_callback", func(t *testing.T) {
		binary := os.Getenv("DIGGER_LEGACY_STOP_RUNNER_TEST_BINARY")
		if binary == "" {
			t.Skip("set DIGGER_LEGACY_STOP_RUNNER_TEST_BINARY for the pinned runner subprocess qualification")
		}
		server := httptest.NewServer(router)
		defer server.Close()
		dir := t.TempDir()
		marker := filepath.Join(dir, "terraform-started")
		require.NoError(t, os.WriteFile(filepath.Join(dir, "digger.yml"), []byte("telemetry: false\nprojects:\n  - name: infra-k8s\n    dir: .\n"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "terraform"), []byte("#!/bin/sh\necho \"$*\" >> \"$DIGGER_TEST_TERRAFORM_MARKER.args\"\nif [ \"$#\" -eq 1 ] && [ \"$1\" = -version ]; then echo \"Terraform v1.10.0\"; exit 0; fi\ntouch \"$DIGGER_TEST_TERRAFORM_MARKER\"\nexit 99\n"), 0700))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestLegacyStoppedRunner$")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "DIGGER_TEST_STOPPED_RUNNER_CHILD=true", "GITHUB_ACTIONS=true", "GITHUB_REPOSITORY=neverendingsupport/infra-terraform", "GITHUB_SERVER_URL=https://github.com", "GITHUB_RUN_ID=999", "GITHUB_RUN_ATTEMPT=1", "DIGGER_TEST_STOPPED_BACKEND="+server.URL, "DIGGER_TEST_STOPPED_TOKEN="+childToken, "DIGGER_TEST_TERRAFORM_MARKER="+marker, "PATH="+dir+":"+os.Getenv("PATH"), "PLAN_UPLOAD_DESTINATION=", "TELEMETRY=false", "DIGGER_OUT="+filepath.Join(dir, "digger-out.log"))
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		var exit *exec.ExitError
		require.True(t, errors.As(err, &exit))
		require.Equal(t, 5, exit.ExitCode(), string(output))
		require.Contains(t, string(output), "Failed to report jobSpec status to backend. Exiting. unexpected status when reporting a project job status: 409")
		_, err = os.Stat(marker)
		require.True(t, os.IsNotExist(err))
	})
	req.Mode = "verify"
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "repository_actions_active")
	export("fenced_pending_verification")

	if scenario != "" {
		active = false
		if strings.HasPrefix(scenario, "after_") {
			response := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			export("verified", scenario+"_initial_verified")
		}
		current, err := db.GetDiggerJob(parent.DiggerJobID)
		require.NoError(t, err)
		current.Status = scheduler.DiggerJobStarted
		current.WorkflowRunAttempt = 2
		current.StatusUpdatedAt = time.Now().UTC()
		current, err = db.AcceptDiggerJobCallback(current, org.ID)
		require.NoError(t, err)
		current.Status = scheduler.DiggerJobFailed
		latestConclusion = "failure"
		if strings.Contains(scenario, "successful") {
			current.Status = scheduler.DiggerJobSucceeded
			latestConclusion = "success"
		}
		current.StatusUpdatedAt = time.Now().UTC().Add(time.Millisecond)
		_, err = db.AcceptDiggerJobCallback(current, org.ID)
		require.NoError(t, err)
		latestAttempt = 2
		_, err = db.ReconcileFailedDependencies(batch.ID)
		require.NoError(t, err)
		req.Mode = "execute"
		response := call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
		export("fenced_pending_verification", scenario+"_pending")
		req.Mode = "verify"
		response = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		export("verified", scenario+"_verified")
		packet, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, 3197, source.CommentID)
		require.NoError(t, err)
		for _, job := range packet.Batches[0].Jobs {
			if job.JobID == child.DiggerJobID {
				out := job.ExecutionOutcome
				require.Equal(t, int64(1), out.Binding.Prerequisite.WorkflowRunAttempt)
				require.Equal(t, int64(2), out.Settlement.CurrentRoot.WorkflowRunAttempt)
				require.Equal(t, latestConclusion, out.Settlement.CurrentRoot.WorkflowConclusion)
			}
		}
		stopped, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.True(t, fence.Equal(*stopped.DependencyRetiredAt))
		require.Equal(t, int64(1), stopped.DependencyStop.Request.FailedPrerequisite.WorkflowRunAttempt)
		return
	}
	active = false
	failReports = true
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"settlement_state":"verified"`)
	committed, err := db.GetDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	require.NotNil(t, committed.VerifiedAt)
	checkpoint := *committed.VerifiedAt
	failReports = false
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	export("verified")
	final, err := db.GetDiggerBatch(&batch.ID)
	require.NoError(t, err)
	require.Equal(t, scheduler.BatchJobFailed, final.Status)
	require.Equal(t, "skipped", patches["/repos/neverendingsupport/infra-terraform/check-runs/129327703323"]["conclusion"])
	require.Contains(t, strings.Join(reportBodies, "\n"), "prior execution unknown")
	// A later retry preserves the historical checkpoint; it grants no new apply permission.
	active = true
	w = call("POST", path, encoded(), "legacy-internal-test", org.ExternalId)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	committed, err = db.GetDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	require.True(t, committed.VerifiedAt.Equal(checkpoint))
}
