package comment_updater

import (
	"strings"
	"testing"

	"github.com/diggerhq/digger/libs/ci"
	"github.com/diggerhq/digger/libs/scheduler"
)

type capturingPRService struct {
	ci.PullRequestService
	body string
}

func (s *capturingPRService) EditComment(_ int, _ string, body string) error {
	s.body = body
	return nil
}

func TestSummaryPreservesSkippedWorkAndLegacyFailure(t *testing.T) {
	service := &capturingPRService{}
	jobs := []scheduler.SerializedJob{
		{DiggerJobId: "failed", ProjectName: "parent", Status: scheduler.DiggerJobFailed, JobString: []byte(`{"job_type":"apply","commands":["digger apply"]}`)},
		{DiggerJobId: "skipped", ProjectName: "child", Status: scheduler.DiggerJobFailed, SkipReason: "prerequisite `failed` failed; never dispatched", JobString: []byte(`{"job_type":"apply","commands":["digger apply"]}`)},
	}
	if err := (BasicCommentUpdater{}).UpdateComment(jobs, 7, service, "report"); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"**parent**", ">failed</a>", "**child** |skipped:", "never dispatched"} {
		if !strings.Contains(service.body, text) {
			t.Fatalf("report missing %q: %s", text, service.body)
		}
	}
}
