package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestCommandRejectionSignedWebhookAndGET(t *testing.T) {
	db := CommandPostgresDB(t)
	previous := models.DB
	models.DB = db
	t.Cleanup(func() { models.DB = previous })
	t.Setenv("DIGGER_DUPLICATE_COMMAND_WINDOW", "0")
	t.Setenv("DIGGER_CONFIG_REPO_CACHE_ENABLED", "0")
	t.Setenv("DIGGER_COMMAND_REJECTION_AFTER", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339))
	t.Setenv("DIGGER_INTERNAL_SECRET", "internal-test")
	var event github.IssueCommentEvent
	require.NoError(t, json.Unmarshal([]byte(issueCommentPayload), &event))
	config := "pr_locks: true\nprojects:\n- name: dev\n  dir: dev\n  workflow: default\n  apply_requirements: [mergeable]\n"
	clone, originalHead := commandPipelineCheckout(t, config)
	event.Repo.CloneURL = github.String(clone)
	event.Comment.Body = github.String("digger apply -p dev\n")
	// GitHub timestamps have one-second resolution.
	stamp := github.Timestamp{Time: time.Now().UTC().Add(time.Second).Truncate(time.Second)}
	event.Comment.CreatedAt = &stamp
	event.Comment.UpdatedAt = &stamp
	org := models.Organisation{Name: "pipeline", ExternalId: "pipeline", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&org).Error)
	repo := models.Repo{Name: "pipeline", RepoFullName: event.Repo.GetFullName(), OrganisationID: org.ID, GithubAppId: 1, GithubAppInstallationId: event.Installation.GetID()}
	require.NoError(t, db.GormDB.Create(&repo).Error)
	_, err := db.CreateGithubInstallationLink(&org, repo.GithubAppInstallationId)
	require.NoError(t, err)
	_, err = db.CreateGithubAppInstallation(repo.GithubAppInstallationId, 1, "owner", 1, repo.RepoFullName)
	require.NoError(t, err)

	var mergeable, holdReview atomic.Bool
	mergeable.Store(true)
	holdReview.Store(true)
	held, release, replyLost := make(chan struct{}), make(chan struct{}), make(chan struct{}, 2)
	var head atomic.Value
	head.Store(originalHead)
	var checks, changingHeadReads atomic.Int64
	var changeHeadDuringCheck atomic.Bool
	transport := commandOriginTransport(func(r *http.Request) (*http.Response, error) {
		path := r.URL.Path
		status, body := 200, "{}"
		switch {
		case strings.HasSuffix(path, "/pulls/2"):
			ok := mergeable.Load()
			state := "dirty"
			if ok {
				state = "clean"
			}
			observedHead := head.Load()
			if changeHeadDuringCheck.Load() && changingHeadReads.Add(1) > 1 {
				observedHead = originalHead
			}
			body = fmt.Sprintf(`{"number":2,"merged":false,"head":{"ref":"feature","sha":%q},"base":{"ref":"main","sha":"base","repo":{"id":%d,"full_name":%q}},"mergeable":%t,"mergeable_state":%q}`, observedHead, event.Repo.GetID(), repo.RepoFullName, ok, state)
		case strings.HasSuffix(path, "/issues/2"):
			body = `{"number":2,"pull_request":{"url":"https://fixture.invalid/pulls/2"}}`
		case strings.HasSuffix(path, "/pulls/2/files"):
			body = `[{"filename":"dev/main.tf"}]`
		case strings.HasSuffix(path, "/pulls/2/merge"):
			status = 404
		case strings.HasSuffix(path, "/contents/digger.yml"):
			body = fmt.Sprintf(`{"type":"file","encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString([]byte(config)))
		case strings.HasSuffix(path, "/reviews"):
			if holdReview.CompareAndSwap(true, false) {
				close(held)
				select {
				case <-release:
				case <-time.After(15 * time.Second):
					return nil, fmt.Errorf("test review gate timed out")
				}
			}
			body = "[]"
		case strings.HasSuffix(path, "/teams"):
			body = "[]"
		case strings.Contains(path, "/compare/"), strings.HasSuffix(path, "/reactions"):
		case strings.HasSuffix(path, "/comments"):
			body = `{"id":9000,"html_url":"https://github.com/example/repo/issues/2#issuecomment-9000"}`
		case strings.Contains(path, "/issues/comments/"):
			b, _ := io.ReadAll(r.Body)
			if strings.Contains(string(b), "requirements checks have failed") {
				select {
				case replyLost <- struct{}{}:
				default:
				}
				status = 503
			}
			body = `{"id":9000}`
		case strings.HasSuffix(path, "/check-runs"):
			body = fmt.Sprintf(`{"id":%d}`, 1000+checks.Add(1))
		case strings.Contains(path, "/check-runs/"):
			body = `{"id":1001,"name":"dev/apply","output":{}}`
		default:
			return nil, fmt.Errorf("unexpected GitHub request %s %s", r.Method, path)
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	provider := webhookCredentialsProvider{DiggerGithubClientMockProvider: utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: transport}}, webhookSecret: "fixture-secret"}
	ci := &commandOriginCI{orgID: org.ID, event: &event, projects: 1}
	send := func(controller DiggerController, kind, app string, event any) {
		router := gin.New()
		router.POST("/github/webhook", controller.GithubAppWebHook)
		payload, err := json.Marshal(event)
		require.NoError(t, err)
		req := httptest.NewRequest("POST", "/github/webhook", strings.NewReader(string(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-GitHub-Event", kind)
		req.Header.Set("X-GitHub-Hook-Installation-Target-ID", app)
		req.Header.Set("X-Hub-Signature-256", signGithubPayload(provider.webhookSecret, string(payload)))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, 202, w.Code)
	}
	deliver := func(app string) {
		send(DiggerController{GithubClientProvider: provider, CiBackendProvider: ci}, "issue_comment", app, &event)
	}
	wait := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		require.Zero(t, background.Wait(ctx))
	}
	router := gin.New()
	router.GET("/api/repos/:repo_id/jobs", middleware.InternalApiAuth(), middleware.HeadersApiAuth(), GetJobsForRepoApi)
	get := func(query string, status int) map[string]json.RawMessage {
		req := httptest.NewRequest("GET", fmt.Sprintf("/api/repos/%d/jobs?%s", repo.ID, query), nil)
		req.Header.Set("Authorization", "Bearer internal-test")
		req.Header.Set("DIGGER_ORG_ID", org.ExternalId)
		req.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
		req.Header.Set("DIGGER_USER_ID", "reader")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, status, w.Code, w.Body.String())
		var result map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		return result
	}
	query := fmt.Sprintf("source_comment_id=%d&pr_number=2", event.Comment.GetID())
	projection := query + "&command_rejection_version=1"
	require.JSONEq(t, "null", string(get(projection, 200)["command_rejection"]))
	deliver("1")
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("first handler did not observe the mergeable PR")
	}
	mergeable.Store(false)
	deliver("1")
	select {
	case <-replyLost:
	case <-time.After(10 * time.Second):
		close(release)
		wait()
		t.Fatal("second handler did not reject")
	}
	close(release)
	wait()
	require.Zero(t, ci.attempts)
	originalID := event.Comment.GetID()
	expected := fmt.Sprintf(`{"version":1,"kind":"rejected_before_dispatch","reason":"not_mergeable","dispatch_fenced":true,"repository_id":%d,"repo_full_name":%q,"pr_number":2,"source_comment_id":%d,"body_sha256":"%x","commit_sha":%q,"command":"apply","app_id":1,"installation_id":%d}`, event.Repo.GetID(), repo.RepoFullName, originalID, sha256.Sum256([]byte(event.Comment.GetBody())), originalHead, repo.GithubAppInstallationId)
	assertReceipt := func() { require.JSONEq(t, expected, string(get(projection, 200)["command_rejection"])) }
	assertReceipt()
	legacy := get(query, 200)
	require.NotContains(t, legacy, "command_rejection")
	require.JSONEq(t, `{"version":1,"complete":true,"batches":[]}`, string(legacy["command_provenance"]))
	for _, q := range []string{"command_rejection_version=1", query + "&command_rejection_version=2", projection + "&command_rejection_version=1"} {
		get(q, 400)
	}

	// Advance the actual fixture branch and re-deliver after mergeability changes.
	dir := strings.TrimSuffix(strings.TrimPrefix(os.Getenv("GIT_CONFIG_KEY_0"), "url.file://"), ".insteadOf")
	cmd := exec.Command("git", "-C", dir, "-c", "user.name=Fixture", "-c", "user.email=f@example.invalid", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "new head")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	out, err = exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	head.Store(strings.TrimSpace(string(out)))
	mergeable.Store(true)
	t.Setenv("DIGGER_COMMAND_REJECTION_AFTER", "")
	deliver("1")
	wait()
	event.Action = github.String("edited")
	event.Comment.Body = github.String("digger apply")
	deliver("1")
	wait()
	require.Zero(t, ci.attempts)
	var count int64
	require.NoError(t, db.GormDB.Model(&models.DiggerBatch{}).Count(&count).Error)
	require.Zero(t, count)
	assertReceipt()
	require.NoError(t, db.GormDB.Unscoped().Model(&models.DiggerLock{}).Count(&count).Error)
	require.Zero(t, count, "a duplicate or redelivery must never take a project lock")
	require.Zero(t, checks.Load(), "a rejected delivery must never write a job check")

	event.Action = github.String("created")
	event.Comment.ID = github.Int64(originalID + 1)
	deliver("1")
	wait()
	require.Equal(t, 1, ci.attempts)
	require.Len(t, ci.runs, 1)
	locks, err := db.GetLocksForOrg(org.ID)
	require.NoError(t, err)
	require.Len(t, locks, 1)
	require.Equal(t, 2, locks[0].LockId)
	mergeable.Store(false)
	t.Setenv("DIGGER_COMMAND_REJECTION_AFTER", "2020-01-01T00:00:00Z")
	deliver("1")
	wait()
	siblingQuery := fmt.Sprintf("source_comment_id=%d&pr_number=2&command_rejection_version=1", event.Comment.GetID())
	sibling := get(siblingQuery, 200)
	require.JSONEq(t, "null", string(sibling["command_rejection"]))
	var manifest models.CommandProvenance
	require.NoError(t, json.Unmarshal(sibling["command_provenance"], &manifest))
	require.Len(t, manifest.Batches, 1)
	assertReceipt()

	// A lost dispatch response can leave work running. A later rejection of
	// that source must preserve the batch even if the PR head didn't move.
	event.Comment.ID = github.Int64(originalID + 2)
	mergeable.Store(true)
	ci.dispatchError = errors.New("lost workflow response")
	deliver("1")
	wait()
	require.Equal(t, 2, ci.attempts)
	mergeable.Store(false)
	deliver("1")
	wait()
	partial := get(fmt.Sprintf("source_comment_id=%d&pr_number=2&command_rejection_version=1", event.Comment.GetID()), 200)
	require.JSONEq(t, "null", string(partial["command_rejection"]))
	require.NoError(t, json.Unmarshal(partial["command_provenance"], &manifest))
	require.Len(t, manifest.Batches, 1)
	require.Equal(t, event.Comment.GetID(), manifest.Batches[0].Origin.SourceCommentID)
	require.Len(t, manifest.Batches[0].Jobs, 1)
	assertReceipt()

	// A check button has a batch ID and no source comment. It can dispatch
	// independently against the same PR and head after a rejection.
	independentCI := &recordingCI{}
	var selected models.DiggerBatch
	var prior models.CommandProvenance
	require.NoError(t, json.Unmarshal(sibling["command_provenance"], &prior))
	require.NoError(t, db.GormDB.First(&selected, "id = ?", prior.Batches[0].BatchID).Error)
	selected.Status = scheduler.BatchJobSucceeded
	require.NoError(t, db.GormDB.Model(&selected).Update("status", selected.Status).Error)
	actions := utils.GetActionsForBatch(&selected)
	require.Len(t, actions, 1)
	button := &github.CheckRunEvent{Action: github.String("requested_action"), Repo: event.Repo, Sender: event.Sender, CheckRun: &github.CheckRun{ID: github.Int64(1001)}, RequestedAction: &github.RequestedAction{Identifier: actions[0].Identifier}}
	send(DiggerController{GithubClientProvider: provider, CiBackendProvider: independentCI}, "check_run", "1", button)
	wait()
	require.Len(t, independentCI.runs, 1)
	var origins int64
	require.NoError(t, db.GormDB.Model(&models.BatchCommandOrigin{}).Count(&origins).Error)
	require.EqualValues(t, 2, origins)
	assertReceipt()

	// Missing timestamps, the cutover second itself and an App header that
	// disagrees with the installed App cannot qualify a command. The last
	// delivery changes head between config checkout and the mergeability read.
	for i, modify := range []func() string{
		func() string { event.Comment.CreatedAt = nil; return "1" },
		func() string { t.Setenv("DIGGER_COMMAND_REJECTION_AFTER", stamp.Format(time.RFC3339)); return "1" },
		func() string {
			event.Comment.CreatedAt = &github.Timestamp{Time: stamp.Add(-48 * time.Hour)}
			event.Comment.UpdatedAt = event.Comment.CreatedAt
			return "1"
		},
		func() string { return "99" },
		func() string { changeHeadDuringCheck.Store(true); return "1" },
	} {
		t.Setenv("DIGGER_COMMAND_REJECTION_AFTER", "2020-01-01T00:00:00Z")
		event.Comment.CreatedAt = &stamp
		event.Comment.UpdatedAt = &stamp
		event.Comment.ID = github.Int64(originalID + int64(10+i))
		deliver(modify())
		wait()
		unknown := get(fmt.Sprintf("source_comment_id=%d&pr_number=2&command_rejection_version=1", event.Comment.GetID()), 200)
		require.JSONEq(t, "null", string(unknown["command_rejection"]))
	}

	require.NoError(t, db.GormDB.Model(&models.GithubAppInstallationLink{}).Where("github_installation_id=?", repo.GithubAppInstallationId).Update("status", models.GithubAppInstallationLinkInactive).Error)
	require.JSONEq(t, "null", string(get(projection, 200)["command_rejection"]))
	_, err = db.CreateGithubInstallationLink(&org, 1234)
	require.NoError(t, err)
	_, err = db.CreateGithubAppInstallation(1234, 2, "owner", 1, repo.RepoFullName)
	require.NoError(t, err)
	require.NoError(t, db.GormDB.Model(&repo).Updates(map[string]any{"github_app_id": 2, "github_app_installation_id": 1234}).Error)
	assertReceipt()
	other := models.Organisation{Name: "other", ExternalId: "other", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&other).Error)
	require.NoError(t, db.GormDB.Model(&repo).Update("organisation_id", other.ID).Error)
	org = other
	require.JSONEq(t, "null", string(get(projection, 200)["command_rejection"]))
}
