package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

type stopProviderTransport func(*http.Request) (*http.Response, error)

func (f stopProviderTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLegacyStopProviderRequiresCompleteFreshMergedCheckpoint(t *testing.T) {
	cases := []string{"valid", "open", "not_merged", "wrong_body", "wrong_issue", "wrong_repository", "parent_rerun", "parent_success", "head_changes", "missing_count", "pagination", "unavailable"}
	for _, s := range models.DependencyStopIncompleteStatuses {
		cases = append(cases, "active_"+s)
	}
	for _, scenario := range cases {
		t.Run(scenario, func(t *testing.T) {
			batch := &models.DiggerBatch{RepoOwner: "neverendingsupport", RepoName: "infra-terraform", RepoFullName: "neverendingsupport/infra-terraform", PrNumber: 3197}
			req := models.DependencyStopRequest{PRNumber: 3197, SourceCommentID: 6746866728, BodySHA256: "ba7707804cf8a8400a1bac3f2d7f291966476b77974b7ca2979ecad1eda5cc02", FailedPrerequisite: models.DependencyStopPrerequisite{WorkflowRunID: 42665476673, WorkflowRunAttempt: 1}}
			seen := map[string]int{}
			prReads := 0
			transport := stopProviderTransport(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "GET", r.Method)
				body := `{}`
				status := http.StatusOK
				header := http.Header{"Content-Type": {"application/json"}}
				switch {
				case strings.HasSuffix(r.URL.Path, "/issues/comments/6746866728"):
					source, issue := "digger apply", "3197"
					if scenario == "wrong_body" {
						source = "digger plan"
					}
					if scenario == "wrong_issue" {
						issue = "3198"
					}
					body = fmt.Sprintf(`{"id":6746866728,"body":%q,"issue_url":"https://api.github.com/repos/neverendingsupport/infra-terraform/issues/%s"}`, source, issue)
				case strings.HasSuffix(r.URL.Path, "/pulls/3197"):
					prReads++
					state, merged, head := "closed", true, strings.Repeat("1", 40)
					if scenario == "open" {
						state = "open"
					}
					if scenario == "not_merged" {
						merged = false
					}
					if scenario == "head_changes" && prReads > 1 {
						head = strings.Repeat("3", 40)
					}
					body = fmt.Sprintf(`{"number":3197,"state":%q,"merged":%t,"merge_commit_sha":%q,"head":{"sha":%q},"base":{"repo":{"id":954216504,"full_name":"neverendingsupport/infra-terraform"}}}`, state, merged, strings.Repeat("2", 40), head)
				case strings.HasSuffix(r.URL.Path, "/actions/runs/42665476673"):
					attempt, repo, conclusion := 1, 954216504, "failure"
					if scenario == "parent_rerun" {
						attempt = 2
					}
					if scenario == "wrong_repository" {
						repo++
					}
					if scenario == "parent_success" {
						conclusion = "success"
					}
					body = fmt.Sprintf(`{"id":42665476673,"run_attempt":%d,"status":"completed","conclusion":%q,"repository":{"id":%d,"full_name":"neverendingsupport/infra-terraform"}}`, attempt, conclusion, repo)
				case strings.HasSuffix(r.URL.Path, "/actions/runs"):
					q := r.URL.Query()
					require.Equal(t, "100", q.Get("per_page"))
					require.Equal(t, "1", q.Get("page"))
					require.Len(t, q, 3, "no head/branch/date filter may hide active work")
					s := q.Get("status")
					seen[s]++
					body = `{"total_count":0,"workflow_runs":[]}`
					if scenario == "active_"+s {
						body = `{"total_count":1,"workflow_runs":[{"id":999}]}`
					}
					if scenario == "missing_count" {
						body = `{"workflow_runs":[]}`
					}
					if scenario == "pagination" {
						header.Set("Link", `<https://api.github.com/repos/neverendingsupport/infra-terraform/actions/runs?page=2>; rel="next"`)
					}
					if scenario == "unavailable" {
						status = http.StatusServiceUnavailable
					}
				default:
					t.Fatalf("unexpected provider request %s", r.URL)
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client := github.NewClient(&http.Client{Transport: transport})
			evidence, err := VerifyDependencyStopQuiescence(context.Background(), client, batch, req, 954216504)
			if scenario == "valid" {
				require.NoError(t, err)
				require.True(t, evidence.ActionsComplete)
				require.Equal(t, strings.Repeat("1", 40), evidence.PRHeadSHA)
				require.Equal(t, 2, prReads)
				for _, s := range models.DependencyStopIncompleteStatuses {
					require.Equal(t, 1, seen[s])
				}
			} else {
				require.Error(t, err)
				require.Nil(t, evidence)
			}
		})
	}
}
