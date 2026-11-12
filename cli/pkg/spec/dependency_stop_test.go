package spec

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	comment_summary "github.com/diggerhq/digger/libs/comment_utils/summary"
	"github.com/diggerhq/digger/libs/scheduler"
	jobspec "github.com/diggerhq/digger/libs/spec"
	"github.com/stretchr/testify/require"
)

// RunSpec is unchanged from the legacy producer's pinned 2258d2 source. Exercise
// its real process-exit boundary rather than replacing its error handler.
func TestLegacyStoppedRunner(t *testing.T) {
	if os.Getenv("DIGGER_TEST_STOPPED_RUNNER_CHILD") == "true" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
		pr := 3197
		spec := jobspec.Spec{JobId: "wWyMzTVZcl", Job: scheduler.JobJson{ProjectName: "infra-k8s", ProjectDir: ".", PullRequestNumber: &pr, JobType: "apply", Commands: []string{"digger apply"}, Namespace: "neverendingsupport/infra-terraform"}, VCS: jobspec.VcsSpec{VcsType: "noop", RepoOwner: "neverendingsupport", RepoName: "infra-terraform"}, Lock: jobspec.LockSpec{LockType: "noop"}, Reporter: jobspec.ReporterSpec{ReporterType: "noop"}, Backend: jobspec.BackendSpec{BackendType: "backend", BackendHostname: os.Getenv("DIGGER_TEST_STOPPED_BACKEND"), BackendJobToken: os.Getenv("DIGGER_TEST_STOPPED_TOKEN")}, Policy: jobspec.PolicySpec{PolicyType: "http"}}
		err := RunSpec(spec, jobspec.VCSProviderBasic{}, jobspec.JobSpecProvider{}, jobspec.LockProvider{}, jobspec.ReporterProvider{}, jobspec.BackendApiProvider{}, jobspec.BasicPolicyProvider{}, jobspec.PlanStorageProvider{}, jobspec.VariablesProvider{}, comment_summary.CommentUpdaterProviderBasic{})
		t.Fatalf("stopped runner returned without exiting: %v", err)
	}
	statuses := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/repos/infra-terraform/projects/infra-k8s/jobs/wWyMzTVZcl/set-status", r.URL.Path)
		var payload struct {
			Status string `json:"status"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		statuses = append(statuses, payload.Status)
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "digger.yml"), []byte("telemetry: false\nprojects:\n  - name: infra-k8s\n    dir: .\n"), 0600))
	marker := filepath.Join(dir, "terraform-started")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "terraform"), []byte("#!/bin/sh\necho \"$*\" >> \"$DIGGER_TEST_TERRAFORM_MARKER.args\"\nif [ \"$#\" -eq 1 ] && [ \"$1\" = -version ]; then echo \"Terraform v1.10.0\"; exit 0; fi\ntouch \"$DIGGER_TEST_TERRAFORM_MARKER\"\nexit 99\n"), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLegacyStoppedRunner$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DIGGER_TEST_STOPPED_RUNNER_CHILD=true", "GITHUB_ACTIONS=true", "GITHUB_REPOSITORY=neverendingsupport/infra-terraform", "GITHUB_SERVER_URL=https://github.com", "GITHUB_RUN_ID=999", "GITHUB_RUN_ATTEMPT=1", "DIGGER_TEST_STOPPED_BACKEND="+server.URL, "DIGGER_TEST_STOPPED_TOKEN=test-token", "DIGGER_TEST_TERRAFORM_MARKER="+marker, "PATH="+dir+":"+os.Getenv("PATH"), "PLAN_UPLOAD_DESTINATION=", "TELEMETRY=false", "DIGGER_OUT="+filepath.Join(dir, "digger-out.log"))
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	var exit *exec.ExitError
	require.True(t, errors.As(err, &exit))
	require.Equal(t, 5, exit.ExitCode(), string(output))
	require.Contains(t, string(output), "Failed to report jobSpec status to backend. Exiting")
	require.Equal(t, []string{"started", "failed"}, statuses, "setup reached the real admission boundary and its error path")
	_, err = os.Stat(marker)
	trace, _ := os.ReadFile(marker + ".args")
	require.True(t, os.IsNotExist(err), "Terraform init/plan/apply must not start after rejected admission; args=%s output=%s", trace, output)
	require.Equal(t, "-version", strings.TrimSpace(string(trace)), "the legacy startup version probe is the only permitted Terraform invocation")
}
