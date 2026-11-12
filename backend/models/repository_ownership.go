package models

import (
	"errors"
	"strings"

	"gorm.io/gorm/clause"
)

// EnsureProducerRepository records ownership from an authenticated VCS producer.
// Callbacks and caller-supplied project reports must not create this authority.
func (db *Database) EnsureProducerRepository(orgID uint, vcs DiggerVCSType, fullName, owner, name string) error {
	if orgID == 0 || fullName == "" || owner == "" || name == "" || (vcs != DiggerVCSGitlab && vcs != DiggerVCSBitbucket) {
		return errors.New("invalid producer repository identity")
	}
	var existing []Repo
	if err := db.GormDB.Where("organisation_id = ? AND repo_full_name = ? AND vcs = ?", orgID, fullName, vcs).Limit(2).Find(&existing).Error; err != nil {
		return err
	}
	if len(existing) == 1 {
		return nil
	}
	if len(existing) > 1 {
		return errors.New("duplicate producer repository ownership")
	}
	repo := Repo{Name: strings.ReplaceAll(fullName, "/", "-"), RepoFullName: fullName, RepoOrganisation: owner, RepoName: name, OrganisationID: orgID, VCS: vcs}
	result := db.GormDB.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "name"}, {Name: "organisation_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"repo_full_name": fullName, "repo_organisation": owner, "repo_name": name, "vcs": vcs,
		}),
		Where: clause.Where{Exprs: []clause.Expression{clause.Expr{
			SQL: "repos.deleted_at IS NULL AND (COALESCE(repos.repo_full_name, '') = '' OR (repos.repo_full_name = ? AND repos.vcs = ?))", Vars: []interface{}{fullName, vcs},
		}}},
	}).Create(&repo)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("producer repository conflicts with an existing ownership binding")
	}
	return nil
}
