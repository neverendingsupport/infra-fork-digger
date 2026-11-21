package models

import (
	"time"

	"gorm.io/gorm"
)

// Version zero reserves the right to construct jobs. Version one permanently
// rejects this source. Both use the same unique key across installations and heads.
func (db *Database) AdmitApply(source GitHubCommandSource) (bool, error) {
	var decision struct{ Version int }
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO command_decisions (repository_id, source_comment_id)
            VALUES (?, ?) ON CONFLICT DO NOTHING`, source.RepositoryID, source.CommentID).Error; err != nil {
			return err
		}
		return tx.Table("command_decisions").Select("version").Where(
			"repository_id = ? AND source_comment_id = ?", source.RepositoryID, source.CommentID).Take(&decision).Error
	})
	return decision.Version == 0 && err == nil, err
}

type CommandRejection struct {
	CommandProvenanceOrigin
	Version        int    `json:"version"`
	Kind           string `json:"kind"`
	Reason         string `json:"reason"`
	DispatchFenced bool   `json:"dispatch_fenced"`
	AppID          int64  `json:"app_id"`
	InstallationID int64  `json:"installation_id"`
}

// Resolve the repository's current App through both the active installation
// and the organisation link. The immutable receipt retains its original App.
const activeCommandRepo = ` FROM repos r
    JOIN organisations o ON o.id = r.organisation_id AND o.deleted_at IS NULL
    WHERE r.deleted_at IS NULL AND r.vcs = 'github'
    AND EXISTS (SELECT 1 FROM github_app_installation_links l
        JOIN github_app_installations i ON i.github_installation_id = l.github_installation_id
        WHERE l.organisation_id = r.organisation_id AND i.repo = r.repo_full_name
        AND i.github_app_id = r.github_app_id AND i.github_installation_id = r.github_app_installation_id
        AND i.status = ? AND l.status = ? AND l.deleted_at IS NULL AND i.deleted_at IS NULL)`

func (db *Database) RejectUnmergeableApply(orgID uint, origin CommandProvenanceOrigin, appID, installationID int64, created time.Time) (bool, error) {
	result := db.GormDB.Exec(`INSERT INTO command_decisions
        (repository_id, source_comment_id, version, organisation_id, repo_id, repo_full_name,
         pr_number, body_sha256, commit_sha, app_id, installation_id)
        SELECT ?, ?, 1, r.organisation_id, MIN(r.id), r.repo_full_name, ?, ?, ?, ?, ?`+activeCommandRepo+`
        AND r.github_app_id = ? AND r.github_app_installation_id = ? AND r.organisation_id = ? AND r.repo_full_name = ?
        AND ? > (SELECT installed_at FROM command_rejection_epoch WHERE id = 1)
        AND NOT EXISTS (SELECT 1 FROM batch_command_origins WHERE repository_id = ? AND source_comment_id = ?)
        GROUP BY r.organisation_id, r.repo_full_name HAVING COUNT(*) = 1
        ON CONFLICT DO NOTHING`, origin.RepositoryID, origin.SourceCommentID, origin.PrNumber, origin.BodySHA256,
		origin.CommitSHA, appID, installationID, GithubAppInstallActive, GithubAppInstallationLinkActive,
		appID, installationID, orgID, origin.RepoFullName, created.UTC(), origin.RepositoryID, origin.SourceCommentID)
	return result.RowsAffected == 1, result.Error
}

func (db *Database) getCommandRejection(orgID uint, repo *Repo, pr int, commentID int64) (*CommandRejection, error) {
	var receipts []CommandRejection
	result := db.GormDB.Raw(`SELECT d.repository_id, d.repo_full_name, d.pr_number, d.source_comment_id,
        d.body_sha256, d.commit_sha, d.version, d.app_id, d.installation_id
        FROM command_decisions d WHERE d.version = 1 AND d.organisation_id = ? AND d.repo_id = ?
          AND d.repo_full_name = ? AND d.pr_number = ? AND d.source_comment_id = ?
          AND EXISTS (SELECT 1`+activeCommandRepo+`
            AND r.id = d.repo_id AND r.organisation_id = d.organisation_id AND r.repo_full_name = d.repo_full_name)
          AND NOT EXISTS (SELECT 1 FROM batch_command_origins b
              WHERE b.repository_id = d.repository_id AND b.source_comment_id = d.source_comment_id)
        LIMIT 2`, orgID, repo.ID, repo.RepoFullName, pr, commentID, GithubAppInstallActive, GithubAppInstallationLinkActive).Scan(&receipts)
	if result.Error != nil || result.RowsAffected != 1 {
		return nil, result.Error
	}
	receipt := receipts[0]
	receipt.Kind = "rejected_before_dispatch"
	receipt.Reason = "not_mergeable"
	receipt.DispatchFenced = true
	receipt.Command = "apply"
	return &receipt, nil
}
