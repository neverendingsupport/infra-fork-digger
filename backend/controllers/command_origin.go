package controllers

import (
	"crypto/sha256"
	"fmt"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
)

// githubCommandSource retains the exact created comment accepted by the verified
// webhook handler. Reporting comment IDs and later comment edits are not origins.
func githubCommandSource(event *github.IssueCommentEvent) (*models.GitHubCommandSource, error) {
	if event == nil || event.GetAction() != "created" || event.GetRepo().GetID() <= 0 ||
		event.GetComment().GetID() <= 0 || event.GetComment().GetBody() == "" ||
		event.GetIssue().GetNumber() <= 0 || !event.GetIssue().IsPullRequest() {
		return nil, fmt.Errorf("a created PR comment with repository and comment IDs is required")
	}
	digest := sha256.Sum256([]byte(event.GetComment().GetBody()))
	return &models.GitHubCommandSource{
		RepositoryID: event.GetRepo().GetID(),
		CommentID:    event.GetComment().GetID(),
		BodySHA256:   fmt.Sprintf("%x", digest),
	}, nil
}
