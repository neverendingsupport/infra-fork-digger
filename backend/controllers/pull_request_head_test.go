package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

const staleSha = "1111111111111111111111111111111111111111"

type recordingCI struct {
	mu   sync.Mutex
	runs []spec.Spec
}

func (c *recordingCI) GetCiBackend(ci_backends.CiBackendOptions) (ci_backends.CiBackend, error) {
	return c, nil
}

func (c *recordingCI) GetWorkflowUrl(spec.Spec) (string, error) { return "", nil }

func (c *recordingCI) TriggerWorkflow(job spec.Spec, _ string, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs = append(c.runs, job)
	return nil
}

// pullRequestHeadHarness runs pull_request events through the handler against
// a fixture repository. GitHub reports the PR at the fixture's commit, ready
// for review; headAfter, when set, is what later PR reads return.
type pullRequestHeadHarness struct {
	t          *testing.T
	headSha    string
	cloneURL   string
	ci         *recordingCI
	orgID      uint
	provider   utils.DiggerGithubClientMockProvider
	checkShas  []string
	headAfter  string
	pullsReads int
	draft      bool
	comments   []string
}

func newPullRequestHeadHarness(t *testing.T) *pullRequestHeadHarness {
	t.Setenv("SEGMENT_API_KEY", "")
	t.Setenv("DIGGER_CONFIG_REPO_CACHE_ENABLED", "0")
	config := "projects:\n- name: dev\n  dir: dev\n  workflow: default\n- name: prod\n  dir: dev\n  workflow: default\n  depends_on: [dev]\n"
	h := &pullRequestHeadHarness{t: t, ci: &recordingCI{}}
	h.cloneURL, h.headSha = commandPipelineCheckout(t, config)
	db, org, _ := commandPipelineDBFor(t, "owner/repo", 77)
	require.NoError(t, db.GormDB.AutoMigrate(&models.DiggerLock{}))
	h.orgID = org.ID
	h.provider = utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: commandOriginTransport(h.respond)}}
	return h
}

func (h *pullRequestHeadHarness) respond(r *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, "{}"
	switch {
	case strings.HasSuffix(r.URL.Path, "/pulls/2"):
		h.pullsReads++
		sha := h.headSha
		if h.headAfter != "" && h.pullsReads > 1 {
			sha = h.headAfter
		}
		body = fmt.Sprintf(`{"number":2,"draft":%t,"head":{"ref":"feature","sha":%q},"base":{"ref":"main","sha":"base-sha"}}`, h.draft, sha)
	case strings.HasSuffix(r.URL.Path, "/pulls/2/files"):
		body = `[{"filename":"dev/main.tf"}]`
	case strings.HasSuffix(r.URL.Path, "/pulls/2/merge"), strings.HasSuffix(r.URL.Path, "/issues/2"):
		status = http.StatusNotFound
	case strings.Contains(r.URL.Path, "/contents/digger."):
		status = http.StatusNotFound
	case strings.Contains(r.URL.Path, "/issues/") && strings.HasSuffix(r.URL.Path, "/comments"):
		var comment struct {
			Body string `json:"body"`
		}
		raw, _ := io.ReadAll(r.Body)
		require.NoError(h.t, json.Unmarshal(raw, &comment))
		h.comments = append(h.comments, comment.Body)
		body = `{"id":9000,"html_url":"https://github.com/owner/repo/issues/2#issuecomment-9000"}`
	case strings.Contains(r.URL.Path, "/issues/comments/"):
		body = `{"id":9000}`
	case strings.HasSuffix(r.URL.Path, "/check-runs"):
		var check struct {
			HeadSHA string `json:"head_sha"`
		}
		raw, _ := io.ReadAll(r.Body)
		require.NoError(h.t, json.Unmarshal(raw, &check))
		h.checkShas = append(h.checkShas, check.HeadSHA)
		body = fmt.Sprintf(`{"id":%d}`, 1000+len(h.checkShas))
	case strings.Contains(r.URL.Path, "/check-runs/"):
		body = `{"id":1001}`
	default:
		return nil, fmt.Errorf("unexpected GitHub request: %s %s", r.Method, r.URL.Path)
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// send delivers an event whose payload carries the given head and draft state.
func (h *pullRequestHeadHarness) send(action string, payloadSha string, payloadDraft bool) {
	event := &github.PullRequestEvent{
		Action: github.String(action),
		PullRequest: &github.PullRequest{
			Number: github.Int(2),
			Draft:  github.Bool(payloadDraft),
			Head:   &github.PullRequestBranch{Ref: github.String("feature"), SHA: github.String(payloadSha)},
			Base:   &github.PullRequestBranch{Ref: github.String("main")},
		},
		Repo: &github.Repository{
			Name: github.String("repo"), FullName: github.String("owner/repo"), Owner: &github.User{Login: github.String("owner")},
			CloneURL: github.String(h.cloneURL), DefaultBranch: github.String("main"),
		},
		Installation: &github.Installation{ID: github.Int64(77)},
		Sender:       &github.User{Login: github.String("dev")},
	}
	require.NoError(h.t, handlePullRequestEvent(h.provider, event, h.ci, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Zero(h.t, background.Wait(ctx))
}

func TestPullRequestEventPlansCurrentHead(t *testing.T) {
	h := newPullRequestHeadHarness(t)

	// The push's payload still says draft; the PR is ready by the time it's handled.
	h.send("synchronize", h.headSha, true)
	require.Len(t, h.ci.runs, 1, "only the first layer dispatches")
	require.Equal(t, h.headSha, h.ci.runs[0].Job.Commit)
	require.NotEmpty(t, h.checkShas)
	for _, sha := range h.checkShas {
		require.Equal(t, h.headSha, sha)
	}

	// ready_for_review for the same commit is a duplicate.
	h.send("ready_for_review", h.headSha, false)
	require.Len(t, h.ci.runs, 1)
}

func TestPullRequestEventDoesNotRunAnotherCommitAsItsSender(t *testing.T) {
	h := newPullRequestHeadHarness(t)

	// Someone pushed after this ready_for_review was sent; its sender didn't
	// act on the new head.
	h.send("ready_for_review", staleSha, false)

	require.Empty(t, h.ci.runs)
	require.Empty(t, h.checkShas)
	require.Len(t, h.comments, 1)
	require.Contains(t, h.comments[0], "comment `digger plan`")
}

func TestPullRequestEventSkipsWhenHeadMovesDuringConfigLoad(t *testing.T) {
	h := newPullRequestHeadHarness(t)
	h.headAfter = staleSha

	h.send("synchronize", h.headSha, false)
	require.Empty(t, h.ci.runs, "the newer push's own event plans it")
	require.Empty(t, h.checkShas)
}

func TestPullRequestEventUnlocksWhenHeadMovesDuringConfigLoad(t *testing.T) {
	h := newPullRequestHeadHarness(t)
	h.headAfter = staleSha
	h.draft = true
	_, err := models.DB.CreateDiggerLock("owner/repo#dev", 2, h.orgID)
	require.NoError(t, err)

	h.send("converted_to_draft", h.headSha, true)

	var locks []models.DiggerLock
	require.NoError(t, models.DB.GormDB.Find(&locks).Error)
	require.Empty(t, locks, "the push's event won't release a draft's locks, so this one must")
	require.Empty(t, h.ci.runs)
}
