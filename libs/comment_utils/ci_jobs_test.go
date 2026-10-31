package comment_utils

import "testing"

func TestWorkflowAttemptRequiresExactGitHubExecution(t *testing.T) {
	for _, tc := range []struct {
		name, actions, run, attempt string
		wantRun, wantAttempt        int64
	}{
		{"first", "true", "12345", "1", 12345, 1},
		{"rerun", "true", "12345", "3", 12345, 3},
		{"outside github", "false", "12345", "1", 0, 0},
		{"legacy", "true", "12345", "", 0, 0},
		{"no run", "true", "", "1", 0, 0},
		{"malformed run", "true", "123x", "1", 0, 0},
		{"malformed attempt", "true", "12345", "1.0", 0, 0},
		{"zero", "true", "12345", "0", 0, 0},
		{"negative", "true", "-12345", "1", 0, 0},
		{"overflow", "true", "9223372036854775808", "1", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GITHUB_ACTIONS", tc.actions)
			t.Setenv("GITHUB_RUN_ID", tc.run)
			t.Setenv("GITHUB_RUN_ATTEMPT", tc.attempt)
			run, attempt := GetWorkflowAttempt()
			if run != tc.wantRun || attempt != tc.wantAttempt {
				t.Fatalf("got (%d, %d), want (%d, %d)", run, attempt, tc.wantRun, tc.wantAttempt)
			}
		})
	}
}
