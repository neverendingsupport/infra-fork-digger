package comment_utils

import (
	"fmt"
	"os"
	"strconv"
)

func isGitHubActions() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true"
}

// GetWorkflowAttempt identifies this execution, not the latest attempt visible
// when a consumer later reads the run. Non-GitHub and older environments are unknown.
func GetWorkflowAttempt() (int64, int64) {
	if !isGitHubActions() {
		return 0, 0
	}
	run, runErr := strconv.ParseInt(os.Getenv("GITHUB_RUN_ID"), 10, 64)
	attempt, attemptErr := strconv.ParseInt(os.Getenv("GITHUB_RUN_ATTEMPT"), 10, 64)
	if runErr != nil || attemptErr != nil || run <= 0 || attempt <= 0 {
		return 0, 0
	}
	return run, attempt
}

func GetWorkflowUrl() string {
	if isGitHubActions() {
		githubServerURL := os.Getenv("GITHUB_SERVER_URL")  // e.g., https://github.com
		githubRepository := os.Getenv("GITHUB_REPOSITORY") // e.g., diggerhq/demo-opentofu
		githubRunID := os.Getenv("GITHUB_RUN_ID")          // numeric run ID
		workflowURL := fmt.Sprintf("%s/%s/actions/runs/%s", githubServerURL, githubRepository, githubRunID)
		return workflowURL
	} else {
		return "#"
	}
}
