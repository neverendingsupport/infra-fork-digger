package controllers

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
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

const alreadyRejectedReply = ":information_source: This `digger apply` was already rejected before it ran, so it won't run. Comment `digger apply` again to retry."

// applyRefusal records why an apply comment won't run, so a delayed delivery
// of the same comment can't run it later. For other commands it does nothing.
type applyRefusal struct {
	command     scheduler.DiggerCommand
	source      *models.GitHubCommandSource
	headerAppID int64
	rejection   models.ApplyRejection
}

// earlier returns a reply when this apply comment must not proceed: it was
// already rejected, or that can't be checked.
func (a applyRefusal) earlier(sourceErr error) (string, error) {
	if sourceErr != nil {
		return ":x: Couldn't identify this `digger apply` comment, so it was skipped.", fmt.Errorf("identify command source: %w", sourceErr)
	}
	rejected, err := models.DB.IsCommandSourceRejected(*a.source)
	if err != nil {
		slog.Error("Could not check for an earlier rejection", "commentId", a.source.CommentID, "error", err)
		return ":x: Couldn't check whether this `digger apply` was already rejected, so it was skipped. Comment `digger apply` again to retry.", fmt.Errorf("check command rejection: %w", err)
	}
	if rejected {
		slog.Info("Skipping a command that was already rejected", "commentId", a.source.CommentID)
		return alreadyRejectedReply, nil
	}
	return "", nil
}

// record stores the refusal before the caller replies, so it survives a lost
// reply. The returned note completes the reply when the receipt wasn't stored.
func (a applyRefusal) record(reason string) string {
	if a.command != scheduler.DiggerCommandApply || a.source == nil {
		return ""
	}
	r := a.rejection
	r.Reason = reason
	appID, err := installedAppID(r.GithubInstallationID, r.RepoFullName, a.headerAppID)
	if err == nil {
		r.GithubAppID = appID
		err = models.DB.RecordApplyRejection(*a.source, r)
	}
	switch {
	case err == nil:
		return ""
	case errors.Is(err, models.ErrCommandSourceDispatched):
		slog.Warn("Not recording a rejection for a command that already created a batch", "commentId", a.source.CommentID, "reason", reason)
		return " Another delivery of this comment already started, and that run continues."
	default:
		slog.Error("Could not record the command rejection", "commentId", a.source.CommentID, "reason", reason, "error", err)
		return " Digger couldn't record this rejection, so a redelivery of this comment could still run. Comment `digger apply` again instead of retrying this one."
	}
}

// installedAppID takes the App from the stored installation, because the
// webhook's App header isn't covered by every signature configuration.
func installedAppID(installationID int64, repoFullName string, headerAppID int64) (int64, error) {
	installation, err := models.DB.GetGithubAppInstallationByIdAndRepo(installationID, repoFullName)
	if err != nil {
		return 0, err
	}
	if headerAppID != 0 && headerAppID != installation.GithubAppId {
		return 0, fmt.Errorf("webhook App %d differs from installation's App %d", headerAppID, installation.GithubAppId)
	}
	return installation.GithubAppId, nil
}
