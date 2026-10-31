package backendapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatusTransportCarriesTheExecutingAttempt(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "owner/repo")
	t.Setenv("GITHUB_RUN_ID", "5678")
	t.Setenv("GITHUB_RUN_ATTEMPT", "4")
	var report map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/repos/owner-repo/projects/access/jobs/job-1/set-status", r.URL.Path)
		require.Equal(t, "Bearer test-job-token", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&report))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	api := DiggerApi{DiggerHost: server.URL, AuthToken: "test-job-token", HttpClient: server.Client()}
	_, err := api.ReportProjectJobStatus("owner/repo", "access", "job-1", "succeeded", time.Now(), nil, "", "", "", "", nil)
	require.NoError(t, err)
	require.Equal(t, "succeeded", report["status"])
	require.Equal(t, float64(5678), report["workflow_run_id"])
	require.Equal(t, float64(4), report["workflow_run_attempt"])
	require.Equal(t, "https://github.com/owner/repo/actions/runs/5678", report["workflow_url"])
}
