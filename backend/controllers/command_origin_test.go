package controllers

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func commandOriginEvent() *github.IssueCommentEvent {
	return &github.IssueCommentEvent{
		Action:  github.String("created"),
		Repo:    &github.Repository{ID: github.Int64(15)},
		Issue:   &github.Issue{Number: github.Int(9), PullRequestLinks: &github.PullRequestLinks{}},
		Comment: &github.IssueComment{ID: github.Int64(55), Body: github.String("digger plan -p access\n")},
	}
}

func TestCommandOriginPreservesAcceptedCommentAndRevision(t *testing.T) {
	event := commandOriginEvent()
	source, err := githubCommandSource(event)
	require.NoError(t, err)
	require.Equal(t, int64(15), source.RepositoryID)
	require.Equal(t, int64(55), source.CommentID)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte("digger plan -p access\n"))), source.BodySHA256)

	// A duplicate delivery preserves its identity. A later body is a different
	// revision; edited events themselves are never accepted as fresh origins.
	duplicate, err := githubCommandSource(event)
	require.NoError(t, err)
	require.Equal(t, source, duplicate)
	event.Comment.Body = github.String("digger plan -p access")
	changed, err := githubCommandSource(event)
	require.NoError(t, err)
	require.NotEqual(t, source.BodySHA256, changed.BodySHA256)
	require.Equal(t, source.CommentID, changed.CommentID)
}

func TestCommandOriginRejectsMissingOrNonCreatedSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*github.IssueCommentEvent)
	}{
		{"edited", func(e *github.IssueCommentEvent) { e.Action = github.String("edited") }},
		{"deleted", func(e *github.IssueCommentEvent) { e.Action = github.String("deleted") }},
		{"no repository", func(e *github.IssueCommentEvent) { e.Repo = nil }},
		{"invalid repository", func(e *github.IssueCommentEvent) { e.Repo.ID = github.Int64(-1) }},
		{"no comment", func(e *github.IssueCommentEvent) { e.Comment = nil }},
		{"no body", func(e *github.IssueCommentEvent) { e.Comment.Body = nil }},
		{"invalid comment", func(e *github.IssueCommentEvent) { e.Comment.ID = github.Int64(0) }},
		{"no issue", func(e *github.IssueCommentEvent) { e.Issue = nil }},
		{"ordinary issue", func(e *github.IssueCommentEvent) { e.Issue.PullRequestLinks = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := commandOriginEvent()
			tc.change(event)
			source, err := githubCommandSource(event)
			require.Error(t, err)
			require.Nil(t, source)
		})
	}
	source, err := githubCommandSource(nil)
	require.Error(t, err)
	require.Nil(t, source)
}
