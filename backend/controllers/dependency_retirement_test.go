package controllers

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"io"
	"net/http"
	"net/http/httptest"
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
)

func TestDependencyRecoveryRegisteredAPIAndLateRunner(t *testing.T) {
	for _, scenario := range []string{"callback", "historical_gap", "runner_observed_before_recovery"} {
		t.Run(scenario, func(t *testing.T) { dependencyRecoveryRegisteredAPI(t, scenario) })
	}
}

func dependencyRecoveryRegisteredAPI(t *testing.T, scenario string) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	if scenario == "legacy_unqualified_terminal" {
		t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	}
	gin.SetMode(gin.TestMode)
	t.Setenv("DIGGER_INTERNAL_SECRET", "test-internal-secret")
	event := commandOriginEvent()
	event.Comment.Body = github.String("digger apply")
	event.Repo.FullName = github.String("owner/repo")
	event.Installation = &github.Installation{ID: github.Int64(77)}
	db, org, repo := commandPipelineDB(t, event)
	aggregate, report := "1000", int64(9000)
	batch := models.DiggerBatch{ID: uuid.New(), VCS: models.DiggerVCSGithub, RepoFullName: "owner/repo", RepoOwner: "owner", RepoName: "repo", PrNumber: 9, CommitSha: "original-head", GithubInstallationId: 77, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated, CheckRunId: &aggregate, CommentId: &report, DiggerConfig: "projects:\n  - name: parent\n    dir: parent\n  - name: child\n    dir: child\n    depends_on: [parent]\n"}
	require.NoError(t, db.GormDB.Create(&batch).Error)
	source, err := githubCommandSource(event)
	require.NoError(t, err)
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, *source, []string{"parent", "child"}))
	makeJob := func(project, check string) (*models.DiggerJob, string) {
		token, err := db.CreateDiggerJobToken(org.ID)
		require.NoError(t, err)
		body, err := json.Marshal(scheduler.JobJson{ProjectName: project, JobType: "apply", Commands: []string{"digger apply"}, Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber, Commit: batch.CommitSha, BackendJobToken: token.Value})
		require.NoError(t, err)
		job, err := db.CreateDiggerJob(batch.ID, body, "workflow.yml", &check, nil, "lazy", project)
		require.NoError(t, err)
		return job, token.Value
	}
	parent, parentToken := makeJob("parent", "1001")
	child, childToken := makeJob("child", "1002")
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	// Model a second new replica whose producer setting is still disabled.
	if scenario != "legacy_unqualified_terminal" {
		t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	}
	patches := map[string]map[string]interface{}{}
	reports := []string{}
	rejectAggregate := false
	rejectReport := false
	unexpectedProviderEffects := 0
	var providerMu sync.Mutex
	transport := commandOriginTransport(func(r *http.Request) (*http.Response, error) {
		providerMu.Lock()
		defer providerMu.Unlock()
		status, body := http.StatusOK, "{}"
		switch {
		case strings.HasSuffix(r.URL.Path, "/actions/runs"):
			body = `{"total_count":0,"workflow_runs":[]}`
		case strings.Contains(r.URL.Path, "/check-runs/"):
			if r.Method == "PATCH" {
				var payload map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					return nil, err
				}
				patches[r.URL.Path] = payload
				if rejectAggregate && strings.HasSuffix(r.URL.Path, "/1000") {
					status = http.StatusServiceUnavailable
				}
			}
			body = `{"id":1000,"name":"digger/apply","status":"in_progress","output":{"title":"Apply","summary":"Running"}}`
		case strings.Contains(r.URL.Path, "/issues/comments/"):
			var payload struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				return nil, err
			}
			reports = append(reports, payload.Body)
			if rejectReport {
				status = http.StatusServiceUnavailable
			}
			body = `{"id":9000}`
		default:
			unexpectedProviderEffects++
			return nil, fmt.Errorf("unexpected provider effect: %s %s", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	provider := utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: transport}}
	controller := DiggerController{GithubClientProvider: provider}
	router := gin.New()
	router.POST("/api/repos/:repo_id/batches/:batch_id/reconcile-dependencies", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), controller.ReconcileBatchDependenciesApi)
	router.GET("/api/repos/:repo_id/jobs", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), GetJobsForRepoApi)
	router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), middleware.AccessLevel(models.CliJobAccessType, models.AccessPolicyType, models.AdminPolicyType), controller.SetJobStatusForProject)
	recoveryPath := fmt.Sprintf("/api/repos/%d/batches/%s/reconcile-dependencies", repo.ID, batch.ID)
	recoveryBody := fmt.Sprintf(`{"pr_number":9,"source_comment_id":%d,"commit_sha":"original-head"}`, event.Comment.GetID())
	request := func(method, path, body, token, orgName string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("DIGGER_ORG_ID", orgName)
		req.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
		actor := "operator"
		if scenario == "safe_logging" {
			actor = "operator\r\nforged=record" + strings.Repeat("x", 1000)
		}
		req.Header.Set("DIGGER_USER_ID", actor)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.Zero(t, background.Wait(ctx))
		cancel()
		return w
	}
	require.Equal(t, http.StatusForbidden, request("POST", recoveryPath, recoveryBody, "", org.ExternalId).Code)
	require.Equal(t, http.StatusNotFound, request("POST", recoveryPath, recoveryBody, "test-internal-secret", "other-org").Code)
	for _, changed := range []string{
		strings.ReplaceAll(recoveryBody, "original-head", "changed-head"),
		strings.ReplaceAll(recoveryBody, `"pr_number":9`, `"pr_number":8`),
		strings.ReplaceAll(recoveryBody, fmt.Sprintf(`"source_comment_id":%d`, event.Comment.GetID()), `"source_comment_id":999999`),
	} {
		denied := request("POST", recoveryPath, changed, "test-internal-secret", org.ExternalId)
		require.Equal(t, http.StatusConflict, denied.Code, denied.Body.String())
		untouched, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.Nil(t, untouched.DependencyRetiredAt)
	}
	if scenario == "legacy_unqualified_terminal" {
		path := "/repos/test/projects/parent/jobs/" + parent.DiggerJobID + "/set-status"
		for _, payload := range []string{
			`{"status":"started","timestamp":"2026-11-12T05:00:01Z"}`,
			`{"status":"failed","timestamp":"2026-11-12T05:00:02Z"}`,
		} {
			w := request("POST", path, payload, parentToken, org.ExternalId)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		}
		gotParent, err := db.GetDiggerJob(parent.DiggerJobID)
		require.NoError(t, err)
		require.Equal(t, scheduler.DiggerJobFailed, gotParent.Status)
		require.Zero(t, gotParent.WorkflowRunID)
		require.Equal(t, int64(2), gotParent.CallbackRevision)
		gotChild, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.Zero(t, gotChild.DispatchProtocolVersion)
		require.Nil(t, gotChild.DependencyRetiredAt)
		w := request("GET", fmt.Sprintf("/api/repos/%d/jobs?source_comment_id=%d&pr_number=9", repo.ID, event.Comment.GetID()), "", "test-internal-secret", org.ExternalId)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"manifest_complete":true`)
		require.NotContains(t, w.Body.String(), "execution_outcome")
		return
	}
	legacyDenied := request("POST", "/repos/test/projects/parent/jobs/"+parent.DiggerJobID+"/set-status", `{"status":"failed","timestamp":"2026-11-12T05:00:01Z"}`, parentToken, org.ExternalId)
	require.Equal(t, http.StatusConflict, legacyDenied.Code, "disabled replica must honor persisted batch qualification: "+legacyDenied.Body.String())
	untouchedParent, err := db.GetDiggerJob(parent.DiggerJobID)
	require.NoError(t, err)
	require.Nil(t, untouchedParent.WorkflowDispatchStartedAt)

	if scenario == "astra_stale_parent_attempt" {
		path := "/repos/test/projects/parent/jobs/" + parent.DiggerJobID + "/set-status"
		newer := request("POST", path, `{"status":"started","timestamp":"2026-11-12T05:00:02Z","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":2}`, parentToken, org.ExternalId)
		require.Equal(t, http.StatusOK, newer.Code, newer.Body.String())
		older := request("POST", path, `{"status":"failed","timestamp":"2026-11-12T05:00:01Z","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":1}`, parentToken, org.ExternalId)
		gotParent, err := db.GetDiggerJob(parent.DiggerJobID)
		require.NoError(t, err)
		gotChild, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		gotBatch, err := db.GetDiggerBatch(&batch.ID)
		require.NoError(t, err)
		evidence, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, event.Comment.GetID())
		require.NoError(t, err)
		hasProof := false
		for _, j := range evidence.Batches[0].Jobs {
			if j.ExecutionOutcome != nil {
				hasProof = true
			}
		}
		t.Logf("old callback HTTP=%d; parent status=%d attempt=%d; child retired=%v; aggregate=%d; negative proof=%v", older.Code, gotParent.Status, gotParent.WorkflowRunAttempt, gotChild.DependencyRetiredAt != nil, gotBatch.Status, hasProof)
		require.Equal(t, http.StatusConflict, older.Code, older.Body.String())
		if gotParent.Status != scheduler.DiggerJobStarted || gotParent.WorkflowRunAttempt != 2 {
			t.Error("stale failure overwrote the active newer attempt")
		}
		if gotChild.DependencyRetiredAt != nil {
			t.Error("stale failure permanently retired the newer attempt's dependent")
		}
		if gotBatch.Status == scheduler.BatchJobFailed || gotBatch.Status == scheduler.BatchJobSucceeded {
			t.Error("aggregate became terminal while a newer workflow attempt remains active")
		}
		failed := request("POST", path, `{"status":"failed","timestamp":"2026-11-12T05:00:03Z","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":2}`, parentToken, org.ExternalId)
		require.Equal(t, http.StatusOK, failed.Code, failed.Body.String())
		retired, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.NotNil(t, retired.DependencyRetiredAt)
		fence := *retired.DependencyRetiredAt
		rerun := request("POST", path, `{"status":"started","timestamp":"2026-11-12T05:00:04Z","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":3}`, parentToken, org.ExternalId)
		require.Equal(t, http.StatusOK, rerun.Code, rerun.Body.String())
		retired, err = db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.Equal(t, fence, *retired.DependencyRetiredAt)
		replay := request("POST", path, `{"status":"failed","timestamp":"2026-11-12T05:00:03Z","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":2}`, parentToken, org.ExternalId)
		require.Equal(t, http.StatusConflict, replay.Code)

		return
	}

	if scenario == "astra_cross_org_reader" || scenario == "same_org_reader" || scenario == "other_org_admin" || scenario == "same_org_admin" {
		outsider := models.Organisation{Name: "outsider", ExternalId: "outsider-tenant", ExternalSource: "test"}
		require.NoError(t, db.GormDB.Create(&outsider).Error)
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		require.NoError(t, err)
		t.Setenv("JWT_PUBLIC_KEY", string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})))
		tenant := outsider.ExternalId
		permission := "digger.all.read.*"
		if strings.HasPrefix(scenario, "same_org") {
			tenant = org.ExternalId
		}
		if strings.HasSuffix(scenario, "admin") {
			permission = "digger.all.*"
		}
		token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"tenantId": tenant, "type": "userToken", "permissions": []string{permission}, "exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString(key)
		require.NoError(t, err)
		path := "/repos/test/projects/parent/jobs/" + parent.DiggerJobID + "/set-status"
		w := request("POST", path, `{"status":"failed","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":1}`, token, tenant)
		gotChild, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		gotParent, err := db.GetDiggerJob(parent.DiggerJobID)
		require.NoError(t, err)
		t.Logf("other-org read-only JWT HTTP=%d; parent status=%d; child retired=%v", w.Code, gotParent.Status, gotChild.DependencyRetiredAt != nil)
		if scenario == "same_org_admin" {
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.NotNil(t, gotChild.DependencyRetiredAt)
			require.Equal(t, scheduler.DiggerJobFailed, gotParent.Status)
			require.Equal(t, int64(42665476673), gotParent.WorkflowRunID)
			return
		}
		require.Nil(t, gotParent.WorkflowDispatchStartedAt, "denial must precede any observation")
		require.Equal(t, scheduler.DiggerJobCreated, gotParent.Status)
		if w.Code != http.StatusForbidden {
			t.Error("other organisation's read-only credential was not rejected")
		}
		if gotChild.DependencyRetiredAt != nil {
			t.Error("unauthorized callback permanently retired another organisation's child")
		}
		return
	}
	if scenario == "runner_observed_before_recovery" {
		parent.Status = scheduler.DiggerJobFailed
		require.NoError(t, db.UpdateDiggerJob(parent))
		// A runner reporting created without a run ID may already execute work
		// after this acknowledgement. The empty workflow listing is no proof.
		path := "/repos/test/projects/child/jobs/" + child.DiggerJobID + "/set-status"
		w := request("POST", path, `{"status":"created"}`, childToken, org.ExternalId)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		saved, err := db.GetDiggerJob(child.DiggerJobID)
		require.NoError(t, err)
		require.NotNil(t, saved.WorkflowDispatchStartedAt)
		require.Nil(t, saved.DependencyRetiredAt)
		w = request("POST", recoveryPath, recoveryBody, "test-internal-secret", org.ExternalId)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "execution_active_or_uncertain")
		evidence, err := db.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, event.Comment.GetID())
		require.NoError(t, err)
		require.Equal(t, scheduler.BatchJobCreated, evidence.Batches[0].Status)
		for _, job := range evidence.Batches[0].Jobs {
			require.Nil(t, job.ExecutionOutcome)
		}
		return
	}
	// The registered runner callback persists a real failed parent and retires
	// its untouched child before acknowledging completion.
	parentPath := "/repos/test/projects/parent/jobs/" + parent.DiggerJobID + "/set-status"
	var w *httptest.ResponseRecorder
	if scenario == "historical_gap" {
		// Reproduce process termination after persisting failure but before
		// descendant retirement. Recovery must not require another callback.
		parent.Status = scheduler.DiggerJobFailed
		parent.WorkflowRunID, parent.WorkflowRunAttempt = 42665476673, 1
		require.NoError(t, db.UpdateDiggerJob(parent))
		w = request("POST", recoveryPath, recoveryBody, "test-internal-secret", org.ExternalId)
	} else {
		w = request("POST", parentPath, `{"status":"failed","workflow_url":"https://github.com/owner/repo/actions/runs/42665476673","workflow_run_id":42665476673,"workflow_run_attempt":1}`, parentToken, org.ExternalId)
	}
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	saved, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.NotNil(t, saved.DependencyRetiredAt)
	firstFence := *saved.DependencyRetiredAt
	aggregatePatch := patches["/repos/owner/repo/check-runs/1000"]
	require.Equal(t, "completed", aggregatePatch["status"])
	require.Equal(t, "failure", aggregatePatch["conclusion"])
	require.Equal(t, "skipped", patches["/repos/owner/repo/check-runs/1002"]["conclusion"])
	require.Contains(t, strings.Join(reports, "\n"), "never dispatched")
	for _, status := range []string{"created", "started", "succeeded"} {
		late := request("POST", "/repos/test/projects/child/jobs/"+child.DiggerJobID+"/set-status", fmt.Sprintf(`{"status":%q}`, status), childToken, org.ExternalId)
		require.Equal(t, http.StatusConflict, late.Code, late.Body.String())
	}
	rejectReport = true
	w = request("POST", recoveryPath, recoveryBody, "test-internal-secret", org.ExternalId)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	require.Zero(t, unexpectedProviderEffects, "an uncertain report edit must not create another comment")
	rejectReport = false
	rejectAggregate = true
	w = request("POST", recoveryPath, recoveryBody, "test-internal-secret", org.ExternalId)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	rejectAggregate = false
	for range 2 {
		w = request("POST", recoveryPath, recoveryBody, "test-internal-secret", org.ExternalId)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}
	saved, err = db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, firstFence, *saved.DependencyRetiredAt)
	query := fmt.Sprintf("/api/repos/%d/jobs?source_comment_id=%d&pr_number=9", repo.ID, event.Comment.GetID())
	w = request("GET", query, "", "test-internal-secret", org.ExternalId)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"kind":"never_dispatched"`)
	require.Contains(t, w.Body.String(), `"workflow_run_id":42665476673`)
	require.NotContains(t, w.Body.String(), childToken)
}

func TestAstraDependencyRecoveryRejectsStaleParentAttempt(t *testing.T) {
	dependencyRecoveryRegisteredAPI(t, "astra_stale_parent_attempt")
}

func TestAstraDependencyRetirementRejectsOtherOrgReadOnlyJWT(t *testing.T) {
	dependencyRecoveryRegisteredAPI(t, "astra_cross_org_reader")
}

func TestDependencyCallbackJWTMutationMatrix(t *testing.T) {
	for _, scenario := range []string{"same_org_reader", "other_org_admin", "same_org_admin"} {
		t.Run(scenario, func(t *testing.T) { dependencyRecoveryRegisteredAPI(t, scenario) })
	}
}

func TestDependencyQualificationRegisteredLegacyTerminal(t *testing.T) {
	dependencyRecoveryRegisteredAPI(t, "legacy_unqualified_terminal")
}
