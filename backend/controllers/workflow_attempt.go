package controllers

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/diggerhq/digger/backend/models"
)

// recordWorkflowAttempt pairs the status with the attempt reported by its job
// client. Legacy clients explicitly clear the pair; nearby runs are never guessed.
// Consumers must independently verify this pair against the workflow provider.
func recordWorkflowAttempt(job *models.DiggerJob, request SetJobStatusRequest) error {
	id, attempt := request.WorkflowRunID, request.WorkflowRunAttempt
	if id == 0 && attempt == 0 {
		job.WorkflowRunID, job.WorkflowRunAttempt = 0, 0
		return nil
	}
	if id <= 0 || attempt <= 0 || job.Batch == nil {
		return fmt.Errorf("workflow_run_id and workflow_run_attempt must both be positive")
	}
	parsed, err := url.Parse(request.WorkflowUrl)
	path := "/" + job.Batch.RepoFullName + "/actions/runs/" + strconv.FormatInt(id, 10)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.EscapedPath() != path && parsed.EscapedPath() != path+"/attempts/"+strconv.FormatInt(attempt, 10)) {
		return fmt.Errorf("workflow_url must identify this repository and workflow attempt")
	}
	job.WorkflowRunID, job.WorkflowRunAttempt = id, attempt
	job.WorkflowRunUrl = &request.WorkflowUrl
	return nil
}
