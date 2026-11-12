package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProducerRepositoryOwnershipBindings(t *testing.T) {
	for _, vcs := range []DiggerVCSType{DiggerVCSGitlab, DiggerVCSBitbucket} {
		for _, state := range []string{"new", "placeholder", "bound_elsewhere", "deleted", "duplicate"} {
			t.Run(string(vcs)+"/"+state, func(t *testing.T) {
				db := newCommandOriginDB(t)
				require.NoError(t, db.GormDB.AutoMigrate(&Repo{}))
				org := Organisation{Name: "owner", ExternalId: "owner", ExternalSource: "test"}
				require.NoError(t, db.GormDB.Create(&org).Error)
				if state != "new" {
					repo := Repo{Name: "group-repo", OrganisationID: org.ID}
					if state == "bound_elsewhere" {
						repo.RepoFullName = "group/another-repo"
					}
					if state == "duplicate" {
						repo.RepoFullName, repo.VCS = "group/repo", vcs
					}
					require.NoError(t, db.GormDB.Create(&repo).Error)
					if state == "deleted" {
						require.NoError(t, db.GormDB.Delete(&repo).Error)
					}
					if state == "duplicate" {
						require.NoError(t, db.GormDB.Create(&Repo{Name: "other-name", OrganisationID: org.ID, RepoFullName: "group/repo", VCS: vcs}).Error)
					}
				}
				err := db.EnsureProducerRepository(org.ID, vcs, "group/repo", "group", "repo")
				if state == "bound_elsewhere" || state == "deleted" || state == "duplicate" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.NoError(t, db.EnsureProducerRepository(org.ID, vcs, "group/repo", "group", "repo"))
				var owned []Repo
				require.NoError(t, db.GormDB.Where("organisation_id = ? AND repo_full_name = ?", org.ID, "group/repo").Find(&owned).Error)
				require.Len(t, owned, 1)
				require.Equal(t, vcs, owned[0].VCS)
			})
		}
	}
}
