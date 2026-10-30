package controllers

import (
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
)

func TestWorkflowAttemptMatchesReportedRepositoryAndRun(t *testing.T) {
	for _, tc := range []struct {
		name, url    string
		run, attempt int64
		ok           bool
	}{
		{"run", "https://github.com/owner/repo/actions/runs/55", 55, 2, true},
		{"attempt", "https://github.com/owner/repo/actions/runs/55/attempts/2", 55, 2, true},
		{"legacy", "#", 0, 0, true},
		{"other run", "https://github.com/owner/repo/actions/runs/54", 55, 2, false},
		{"other repo", "https://github.com/other/repo/actions/runs/55", 55, 2, false},
		{"other attempt", "https://github.com/owner/repo/actions/runs/55/attempts/1", 55, 2, false},
		{"missing attempt", "https://github.com/owner/repo/actions/runs/55", 55, 0, false},
		{"negative", "https://github.com/owner/repo/actions/runs/55", -55, 2, false},
		{"missing url", "", 55, 2, false},
		{"query", "https://github.com/owner/repo/actions/runs/55?x=1", 55, 2, false},
		{"encoded", "https://github.com/owner%2frepo/actions/runs/55", 55, 2, false},
		{"http", "http://github.com/owner/repo/actions/runs/55", 55, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &models.DiggerJob{Batch: &models.DiggerBatch{RepoFullName: "owner/repo"}, WorkflowRunID: 44, WorkflowRunAttempt: 1}
			err := recordWorkflowAttempt(job, SetJobStatusRequest{WorkflowUrl: tc.url, WorkflowRunID: tc.run, WorkflowRunAttempt: tc.attempt})
			if !tc.ok {
				require.Error(t, err)
				require.Equal(t, int64(44), job.WorkflowRunID)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.run, job.WorkflowRunID)
			require.Equal(t, tc.attempt, job.WorkflowRunAttempt)
			if tc.run > 0 {
				require.Equal(t, tc.url, *job.WorkflowRunUrl)
			}
		})
	}
}
