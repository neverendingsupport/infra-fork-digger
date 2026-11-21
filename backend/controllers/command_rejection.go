package controllers

import (
	"errors"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/apply_requirements"
	"github.com/google/go-github/v61/github"
)

var qualifiedApply = regexp.MustCompile(`(?i)^\s*digger[ \t]+apply(?:[ \t]+-p[ \t]+[a-z0-9_.-]+)?\s*$`)

func qualifyApplyComment(event *github.IssueCommentEvent) bool {
	if !qualifiedApply.MatchString(event.GetComment().GetBody()) {
		return false
	}
	cutoff := os.Getenv("DIGGER_COMMAND_REJECTION_AFTER")
	if cutoff == "" {
		return false
	}
	after, err := time.Parse(time.RFC3339, cutoff)
	if err != nil {
		slog.Error("digger.apply.invalid_rejection_cutover", "error", err)
		return false
	}
	comment := event.Comment
	if comment.CreatedAt == nil || comment.UpdatedAt == nil || !comment.CreatedAt.Equal(*comment.UpdatedAt) || !comment.CreatedAt.After(after) {
		return false
	}
	return true
}

func recordApplyRejection(event *github.IssueCommentEvent, orgID uint, appID int64, head string, pr *github.PullRequest, checkErr error) error {
	var failed apply_requirements.NotMergeable
	if !errors.As(checkErr, &failed) ||
		pr == nil || pr.Mergeable == nil || pr.GetNumber() != event.Issue.GetNumber() ||
		pr.GetBase().GetRepo().GetID() != event.Repo.GetID() || pr.GetBase().GetRepo().GetFullName() != event.Repo.GetFullName() ||
		head == "" || pr.GetHead().GetSHA() != head || appID <= 0 || event.Installation.GetID() <= 0 {
		return nil
	}
	switch pr.GetMergeableState() {
	case "blocked", "dirty", "behind", "draft":
	default:
		return nil
	}
	source, err := githubCommandSource(event)
	if err != nil {
		return err
	}
	origin := models.CommandProvenanceOrigin{RepositoryID: source.RepositoryID, SourceCommentID: source.CommentID,
		BodySHA256: source.BodySHA256, PrNumber: event.Issue.GetNumber(), RepoFullName: event.Repo.GetFullName(), CommitSHA: head, Command: "apply"}
	saved, err := models.DB.RejectUnmergeableApply(orgID, origin, appID, event.Installation.GetID(), event.Comment.CreatedAt.Time)
	slog.Info("digger.apply.rejection_decided", "source_comment_id", source.CommentID, "commit_sha", head, "saved", saved, "error", err)
	return err
}
