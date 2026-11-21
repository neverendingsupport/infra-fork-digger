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
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

// rejectionHarness drives signed webhooks through the real comment handler
// against the committed PostgreSQL migrations.
type rejectionHarness struct {
	t                      *testing.T
	db                     *models.Database
	org                    models.Organisation
	repo                   models.Repo
	event                  github.IssueCommentEvent
	headSha                string
	ci                     *commandOriginCI
	controller             DiggerController
	secret                 string
	mergeable, failReplies bool
	compareStatus          int
	onCheckRun             func()
	checkRuns              int
	replies                []string
	receiptsAtReply        []int64
}

func newRejectionHarness(t *testing.T, config string) *rejectionHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("SEGMENT_API_KEY", "")
	t.Setenv("DIGGER_CONFIG_REPO_CACHE_ENABLED", "0")
	t.Setenv("DIGGER_INTERNAL_SECRET", "test-internal-secret")
	h := &rejectionHarness{t: t, db: NewPostgresTestDB(t), secret: "test-webhook-secret", compareStatus: http.StatusOK}
	previous := models.DB
	models.DB = h.db
	t.Cleanup(func() { models.DB = previous })
	require.NoError(t, json.Unmarshal([]byte(issueCommentPayload), &h.event))
	cloneURL, headSha := commandPipelineCheckout(t, config)
	h.headSha = headSha
	h.event.Repo.CloneURL = github.String(cloneURL)
	h.event.Comment.Body = github.String("digger apply\n")
	h.org = models.Organisation{Name: "pipeline", ExternalId: "pipeline-id", ExternalSource: "test"}
	require.NoError(t, h.db.GormDB.Create(&h.org).Error)
	h.repo = models.Repo{Name: "pipeline", RepoFullName: h.event.Repo.GetFullName(), OrganisationID: h.org.ID}
	require.NoError(t, h.db.GormDB.Create(&h.repo).Error)
	_, err := h.db.CreateGithubInstallationLink(&h.org, h.event.Installation.GetID())
	require.NoError(t, err)
	_, err = h.db.CreateGithubAppInstallation(h.event.Installation.GetID(), 1, "owner", 1, h.event.Repo.GetFullName())
	require.NoError(t, err)

	checkID := int64(1000)
	transport := commandOriginTransport(func(r *http.Request) (*http.Response, error) {
		status, body := http.StatusOK, "{}"
		switch {
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/issues/comments/"):
			raw, _ := io.ReadAll(r.Body)
			var receipts int64
			require.NoError(t, h.db.GormDB.Model(&models.CommandSourceDisposition{}).Where("disposition = ?", models.CommandSourceRejected).Count(&receipts).Error)
			h.replies = append(h.replies, string(raw))
			h.receiptsAtReply = append(h.receiptsAtReply, receipts)
			if h.failReplies {
				status = http.StatusInternalServerError
			} else {
				body = `{"id":9000}`
			}
		case strings.HasSuffix(r.URL.Path, "/issues/2"):
			body = `{"number":2,"pull_request":{"url":"https://api.github.com/repos/owner/repo/pulls/2"}}`
		case strings.HasSuffix(r.URL.Path, "/pulls/2"):
			state := map[bool]string{true: "clean", false: "dirty"}[h.mergeable]
			body = fmt.Sprintf(`{"number":2,"head":{"ref":"feature","sha":%q},"base":{"ref":"main","sha":"base-sha"},"merged":false,"mergeable":%t,"mergeable_state":%q}`, h.headSha, h.mergeable, state)
		case strings.HasSuffix(r.URL.Path, "/pulls/7"):
			body = `{"number":7,"state":"open"}` // holds the lock in the project_locked case
		case strings.HasSuffix(r.URL.Path, "/pulls/2/files"):
			body = `[{"filename":"dev/main.tf"}]`
		case strings.Contains(r.URL.Path, "/compare/"):
			status, body = h.compareStatus, `{"ahead_by":0,"behind_by":0}`
		case strings.HasSuffix(r.URL.Path, "/contents/digger.yml"):
			body = fmt.Sprintf(`{"type":"file","encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString([]byte(config)))
		case strings.HasSuffix(r.URL.Path, "/reviews"), strings.HasSuffix(r.URL.Path, "/teams"):
			body = "[]"
		case strings.Contains(r.URL.Path, "/issues/") && strings.HasSuffix(r.URL.Path, "/comments"):
			body = `{"id":9000,"html_url":"https://github.com/owner/repo/issues/2#issuecomment-9000"}`
		case strings.Contains(r.URL.Path, "/issues/comments/"):
			body = `{"id":9000}`
		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			h.checkRuns++
			if h.onCheckRun != nil {
				h.onCheckRun()
			}
			checkID++
			body = fmt.Sprintf(`{"id":%d}`, checkID)
		case strings.Contains(r.URL.Path, "/check-runs/"):
			body = `{"id":1001,"name":"dev/apply","status":"in_progress","output":{"title":"Apply","summary":"Running"}}`
		default:
			return nil, fmt.Errorf("unexpected GitHub request: %s %s", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	provider := webhookCredentialsProvider{DiggerGithubClientMockProvider: utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: transport}}, webhookSecret: h.secret}
	h.ci = &commandOriginCI{orgID: h.org.ID, event: &h.event, projects: 1}
	h.controller = DiggerController{GithubClientProvider: provider, CiBackendProvider: h.ci}
	return h
}

func (h *rejectionHarness) deliver(e *github.IssueCommentEvent) string {
	h.t.Helper()
	payload, err := json.Marshal(e)
	require.NoError(h.t, err)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/github/webhook", strings.NewReader(string(payload)))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("X-GitHub-Event", "issue_comment")
	c.Request.Header.Set("X-GitHub-Hook-Installation-Target-ID", "1")
	c.Request.Header.Set("X-Hub-Signature-256", signGithubPayload(h.secret, string(payload)))
	h.controller.GithubAppWebHook(c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Zero(h.t, background.Wait(ctx))
	require.Equal(h.t, http.StatusAccepted, w.Code)
	if len(h.replies) == 0 {
		return ""
	}
	return h.replies[len(h.replies)-1]
}

func (h *rejectionHarness) source(e *github.IssueCommentEvent) models.GitHubCommandSource {
	source, err := githubCommandSource(e)
	require.NoError(h.t, err)
	return *source
}

func (h *rejectionHarness) dispositions() []models.CommandSourceDisposition {
	var rows []models.CommandSourceDisposition
	require.NoError(h.t, h.db.GormDB.Order("source_comment_id").Find(&rows).Error)
	return rows
}

func (h *rejectionHarness) origins() int64 {
	var n int64
	require.NoError(h.t, h.db.GormDB.Model(&models.BatchCommandOrigin{}).Count(&n).Error)
	return n
}

func (h *rejectionHarness) read(commentID int64, include string) (int, map[string]json.RawMessage) {
	h.t.Helper()
	router := gin.New()
	router.GET("/api/repos/:repo_id/jobs", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), GetJobsForRepoApi)
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/repos/%d/jobs?source_comment_id=%d&pr_number=2%s", h.repo.ID, commentID, include), nil)
	req.Header.Set("Authorization", "Bearer test-internal-secret")
	req.Header.Set("DIGGER_ORG_ID", h.org.ExternalId)
	req.Header.Set("DIGGER_ORG_SOURCE", h.org.ExternalSource)
	req.Header.Set("DIGGER_USER_ID", "reader")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	var body map[string]json.RawMessage
	if w.Code == http.StatusOK {
		require.NoError(h.t, json.Unmarshal(w.Body.Bytes(), &body))
	}
	return w.Code, body
}

const rejectionConfig = "pr_locks: false\nprojects:\n- name: dev\n  dir: dev\n  workflow: default\n  apply_requirements: [mergeable]\n"

func TestApplyRejectionReceiptThroughTheHandler(t *testing.T) {
	h := newRejectionHarness(t, rejectionConfig)

	// The rejection reply is lost; the receipt was committed before it.
	h.failReplies = true
	require.Contains(t, h.deliver(&h.event), "apply requirements checks have failed")
	require.Equal(t, []int64{1}, h.receiptsAtReply[len(h.receiptsAtReply)-1:], "the receipt must be committed before the reply")
	require.Zero(t, h.ci.attempts)

	// A delayed redelivery after the PR became mergeable never runs, and is
	// refused before it creates checks or a batch.
	h.failReplies, h.mergeable = false, true
	require.Contains(t, h.deliver(&h.event), "already rejected")
	require.Zero(t, h.ci.attempts)
	require.Zero(t, h.checkRuns)
	var batches int64
	require.NoError(t, h.db.GormDB.Model(&models.DiggerBatch{}).Count(&batches).Error)
	require.Zero(t, batches)

	// A new comment is a new source, so an explicit retry runs normally.
	retry := h.event
	retryComment := *h.event.Comment
	retryComment.ID = github.Int64(h.event.Comment.GetID() + 1)
	retry.Comment = &retryComment
	h.ci.event = &retry
	h.deliver(&retry)
	require.Equal(t, 1, h.ci.attempts)

	code, body := h.read(h.event.Comment.GetID(), "")
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, body, "command_rejection", "readers that don't ask keep the v1 response")
	require.JSONEq(t, `{"version":1,"complete":true,"batches":[]}`, string(body["command_provenance"]))

	code, body = h.read(h.event.Comment.GetID(), "&include=command_rejection")
	require.Equal(t, http.StatusOK, code)
	var projection struct {
		Receipt struct {
			RejectedAt time.Time `json:"rejected_at"`
		} `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(body["command_rejection"], &projection))
	rejectedAt, err := json.Marshal(projection.Receipt.RejectedAt)
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{"version":1,"receipt":{
		"version":1,"kind":"pre_dispatch_rejection","reason":"apply_requirements","dispatch_fenced":true,
		"origin":{"repository_id":%d,"repo_full_name":%q,"pr_number":2,"source_comment_id":%d,"body_sha256":%q,"commit_sha":%q,"command":"apply"},
		"github_app_id":1,"github_installation_id":%d,"rejected_at":%s}}`,
		h.event.Repo.GetID(), h.event.Repo.GetFullName(), h.event.Comment.GetID(),
		fmt.Sprintf("%x", sha256.Sum256([]byte(h.event.Comment.GetBody()))), h.headSha,
		h.event.Installation.GetID(), rejectedAt), string(body["command_rejection"]))

	code, body = h.read(retry.Comment.GetID(), "&include=command_rejection")
	require.Equal(t, http.StatusOK, code)
	require.JSONEq(t, `{"version":1,"receipt":null}`, string(body["command_rejection"]))

	for _, include := range []string{"&include=other", "&include=command_rejection&include=command_rejection"} {
		code, _ = h.read(h.event.Comment.GetID(), include)
		require.Equal(t, http.StatusBadRequest, code, include)
	}
}

func TestApplyRejectionRecordsEachDecision(t *testing.T) {
	for _, tc := range []struct {
		reason, config string
		setup          func(*rejectionHarness)
	}{
		{models.RejectionDraftPR, rejectionConfig, func(h *rejectionHarness) { h.event.Issue.Draft = github.Bool(true) }},
		{models.RejectionProjectLocked, strings.Replace(rejectionConfig, "pr_locks: false", "pr_locks: true", 1), func(h *rejectionHarness) {
			_, err := h.db.CreateDiggerLock(h.event.Repo.GetFullName()+"#dev", 7, h.org.ID)
			require.NoError(h.t, err)
		}},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			h := newRejectionHarness(t, tc.config)
			h.mergeable = true
			tc.setup(h)
			h.deliver(&h.event)
			rows := h.dispositions()
			require.Len(t, rows, 1)
			require.Equal(t, models.CommandSourceRejected, rows[0].Disposition)
			require.Equal(t, tc.reason, rows[0].Reason)
			require.Zero(t, h.ci.attempts)
		})
	}
}

// The early check passed, then another delivery rejected the comment before
// this one wrote its origin. Only the database trigger can stop it now.
func TestApplyRejectionFenceStopsADeliveryInFlight(t *testing.T) {
	h := newRejectionHarness(t, rejectionConfig)
	h.mergeable = true
	h.onCheckRun = func() {
		h.onCheckRun = nil
		require.NoError(t, h.db.RecordApplyRejection(h.source(&h.event), models.ApplyRejection{
			Reason: models.RejectionApplyRequirements, RepoFullName: h.event.Repo.GetFullName(), PrNumber: 2,
			CommitSHA: h.headSha, GithubAppID: 1, GithubInstallationID: h.event.Installation.GetID(),
		}))
	}
	require.Contains(t, h.deliver(&h.event), "already rejected")
	require.Zero(t, h.ci.attempts)
	require.Zero(t, h.origins())
}

// A redelivery refused while the first delivery's batch exists must not
// claim a rejection; the first delivery's work stands.
func TestApplyRejectionKeepsAStartedDelivery(t *testing.T) {
	h := newRejectionHarness(t, rejectionConfig)
	h.mergeable = true
	h.deliver(&h.event)
	require.Equal(t, 1, h.ci.attempts)
	require.Contains(t, h.deliver(&h.event), "Another delivery of this comment already started")
	rows := h.dispositions()
	require.Len(t, rows, 1)
	require.Equal(t, models.CommandSourceBatchCreated, rows[0].Disposition)
	require.Equal(t, int64(1), h.origins())
}

// A GitHub error isn't a decision, so it leaves the comment retryable.
func TestApplyRejectionSkipsGitHubErrors(t *testing.T) {
	h := newRejectionHarness(t, rejectionConfig)
	h.mergeable, h.compareStatus = true, http.StatusInternalServerError
	require.Contains(t, h.deliver(&h.event), "apply requirements checks have failed")
	require.Empty(t, h.dispositions())
	h.compareStatus = http.StatusOK
	h.deliver(&h.event)
	require.Equal(t, 1, h.ci.attempts)
}

// The receipt's App comes from the stored installation. When the webhook names
// another App, nothing is recorded and the reply says the comment is unfenced.
func TestApplyRejectionTakesTheAppFromTheInstallation(t *testing.T) {
	h := newRejectionHarness(t, rejectionConfig)
	require.NoError(t, h.db.GormDB.Model(&models.GithubAppInstallation{}).Where("github_installation_id = ?", h.event.Installation.GetID()).Update("github_app_id", 9).Error)
	require.Contains(t, h.deliver(&h.event), "couldn't record this rejection")
	require.Empty(t, h.dispositions())
}
