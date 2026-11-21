package models

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	CommandSourceBatchCreated = "batch_created"
	CommandSourceRejected     = "rejected"

	RejectionApplyRequirements     = "apply_requirements"
	RejectionProjectLocked         = "project_locked"
	RejectionDuplicateCommand      = "duplicate_command"
	RejectionImpactedProjectsLimit = "impacted_projects_limit"
	RejectionDraftPR               = "draft_pr"

	commandRejectionKind = "pre_dispatch_rejection"
)

var rejectionReasons = []string{RejectionApplyRequirements, RejectionProjectLocked, RejectionDuplicateCommand, RejectionImpactedProjectsLimit, RejectionDraftPR}

var (
	// ErrCommandSourceDispatched means another delivery of the same comment
	// already created a batch, so its work stands and no rejection is recorded.
	ErrCommandSourceDispatched = errors.New("command source already created a batch")
	// ErrCommandSourceRejected means the comment was rejected before dispatch,
	// so the database refused its batch origin.
	ErrCommandSourceRejected = errors.New("command source was rejected before dispatch")
)

// CommandSourceDisposition is the first outcome of a GitHub apply comment. The
// migration's trigger writes batch_created whenever an apply origin is inserted
// and refuses the insert once the comment is rejected. Receipt fields are set
// only on a rejection; read batch_created rows through commandSourceDisposition.
type CommandSourceDisposition struct {
	RepositoryID         int64     `gorm:"primaryKey;autoIncrement:false;<-:create"`
	SourceCommentID      int64     `gorm:"primaryKey;autoIncrement:false;index:idx_command_rejection_source,priority:3;<-:create"`
	Disposition          string    `gorm:"not null;<-:create"`
	CreatedAt            time.Time `gorm:"<-:create"`
	ReceiptVersion       int       `gorm:"<-:create"`
	Reason               string    `gorm:"<-:create"`
	RepoFullName         string    `gorm:"index:idx_command_rejection_source,priority:1,where:disposition = 'rejected';<-:create"`
	PrNumber             int       `gorm:"index:idx_command_rejection_source,priority:2;<-:create"`
	BodySHA256           string    `gorm:"size:64;<-:create"`
	CommitSHA            string    `gorm:"<-:create"`
	GithubAppID          int64     `gorm:"<-:create"`
	GithubInstallationID int64     `gorm:"<-:create"`
}

// ApplyRejection is what the handler knew when it refused an apply. CommitSHA
// is the PR head it loaded the command's configuration from.
type ApplyRejection struct {
	Reason               string
	RepoFullName         string
	PrNumber             int
	CommitSHA            string
	GithubAppID          int64
	GithubInstallationID int64
}

// CommandRejectionProjection is the opt-in sibling of CommandProvenance. A nil
// receipt is not evidence of anything: the command may have run, be running,
// predate receipts, or never have been recorded.
type CommandRejectionProjection struct {
	Version int                      `json:"version"`
	Receipt *CommandRejectionReceipt `json:"receipt"`
}

// CommandRejectionReceipt reuses the provenance origin so consumers can check
// both with one validator. It makes no claim about locks or other side effects.
type CommandRejectionReceipt struct {
	Version              int                     `json:"version"`
	Kind                 string                  `json:"kind"`
	Reason               string                  `json:"reason"`
	DispatchFenced       bool                    `json:"dispatch_fenced"`
	Origin               CommandProvenanceOrigin `json:"origin"`
	GithubAppID          int64                   `json:"github_app_id"`
	GithubInstallationID int64                   `json:"github_installation_id"`
	RejectedAt           time.Time               `json:"rejected_at"`
}

// RecordApplyRejection fences the comment and stores its receipt. A repeat
// delivery keeps the first receipt. If any delivery of the comment already
// created a batch, it returns ErrCommandSourceDispatched.
func (db *Database) RecordApplyRejection(source GitHubCommandSource, r ApplyRejection) error {
	if err := source.validate(); err != nil {
		return err
	}
	if !slices.Contains(rejectionReasons, r.Reason) || strings.TrimSpace(r.RepoFullName) == "" || r.PrNumber <= 0 ||
		strings.TrimSpace(r.CommitSHA) == "" || r.GithubAppID <= 0 || r.GithubInstallationID <= 0 {
		return errors.New("apply rejection requires a reason, repository, PR, head, App and installation")
	}
	err := recordApplyRejection(db.GormDB, rejectionRow(source, r))
	if err == nil {
		slog.Info("digger.command_rejection.recorded", "repository_id", source.RepositoryID, "source_comment_id", source.CommentID, "reason", r.Reason)
	}
	return err
}

func rejectionRow(source GitHubCommandSource, r ApplyRejection) CommandSourceDisposition {
	return CommandSourceDisposition{
		RepositoryID: source.RepositoryID, SourceCommentID: source.CommentID, Disposition: CommandSourceRejected,
		ReceiptVersion: 1, Reason: r.Reason, RepoFullName: r.RepoFullName, PrNumber: r.PrNumber,
		BodySHA256: source.BodySHA256, CommitSHA: r.CommitSHA, GithubAppID: r.GithubAppID, GithubInstallationID: r.GithubInstallationID,
	}
}

// recordApplyRejection claims the comment's single disposition row. On a
// conflict, the committed winner decides the result.
func recordApplyRejection(tx *gorm.DB, row CommandSourceDisposition) error {
	claim := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if claim.Error != nil || claim.RowsAffected == 1 {
		return claim.Error
	}
	disposition, err := commandSourceDisposition(tx, row.RepositoryID, row.SourceCommentID)
	if err != nil || disposition == CommandSourceRejected {
		return err
	}
	return ErrCommandSourceDispatched
}

// IsCommandSourceRejected reports whether this comment can never dispatch.
func (db *Database) IsCommandSourceRejected(source GitHubCommandSource) (bool, error) {
	disposition, err := commandSourceDisposition(db.GormDB, source.RepositoryID, source.CommentID)
	return disposition == CommandSourceRejected, err
}

func commandSourceDisposition(tx *gorm.DB, repositoryID, sourceCommentID int64) (string, error) {
	var dispositions []string
	err := tx.Model(&CommandSourceDisposition{}).Where("repository_id = ? AND source_comment_id = ?", repositoryID, sourceCommentID).
		Limit(1).Pluck("disposition", &dispositions).Error
	if err != nil || len(dispositions) == 0 {
		return "", err
	}
	return dispositions[0], nil
}

func (db *Database) GetCommandRejection(orgID uint, repoFullName string, prNumber int, sourceCommentID int64) (*CommandRejectionProjection, error) {
	if orgID == 0 || repoFullName == "" || prNumber <= 0 || sourceCommentID <= 0 {
		return nil, errors.New("command rejection requires organisation, repository, PR and source comment")
	}
	var rows []CommandSourceDisposition
	err := db.GormDB.Where("disposition = ? AND repo_full_name = ? AND pr_number = ? AND source_comment_id = ?", CommandSourceRejected, repoFullName, prNumber, sourceCommentID).
		Where(installationInOrg("command_source_dispositions"), orgID, GithubAppInstallationLinkActive).
		Limit(1).Find(&rows).Error
	if err != nil {
		return nil, err
	}
	projection := &CommandRejectionProjection{Version: 1}
	if len(rows) == 1 {
		r := rows[0]
		projection.Receipt = &CommandRejectionReceipt{
			Version: r.ReceiptVersion, Kind: commandRejectionKind, Reason: r.Reason, DispatchFenced: true,
			Origin: CommandProvenanceOrigin{
				RepositoryID: r.RepositoryID, RepoFullName: r.RepoFullName, PrNumber: r.PrNumber, SourceCommentID: r.SourceCommentID,
				BodySHA256: r.BodySHA256, CommitSHA: r.CommitSHA, Command: scheduler.DiggerCommandApply,
			},
			GithubAppID: r.GithubAppID, GithubInstallationID: r.GithubInstallationID, RejectedAt: r.CreatedAt.UTC(),
		}
	}
	return projection, nil
}
