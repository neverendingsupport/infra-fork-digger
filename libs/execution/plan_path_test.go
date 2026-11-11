package execution

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStoredPlanFilePathIsKeyedByPullRequestCommit(t *testing.T) {
	pr := 7
	old := ProjectPathProvider{ProjectNamespace: "acme/infra", ProjectName: "dev", PRNumber: &pr, Commit: "aaa"}
	current := old
	current.Commit = "bbb"

	assert.Equal(t, "acme-infra-7-dev-aaa.tfplan", old.StoredPlanFilePath())
	assert.NotEqual(t, old.StoredPlanFilePath(), current.StoredPlanFilePath())
	assert.Equal(t, "dev-aaa", old.ArtifactName())
	assert.NotEqual(t, old.ArtifactName(), current.ArtifactName())
}

func TestStoredPlanFilePathWithoutCommitKeepsItsOldName(t *testing.T) {
	pr := 7
	p := ProjectPathProvider{ProjectNamespace: "acme/infra", ProjectName: "dev", PRNumber: &pr}
	assert.Equal(t, "acme-infra-7-dev.tfplan", p.StoredPlanFilePath())
	assert.Equal(t, "dev", p.ArtifactName())
}

func TestStoredPlanFilePathWithoutPullRequest(t *testing.T) {
	p := ProjectPathProvider{ProjectNamespace: "acme/infra", ProjectName: "dev"}
	assert.Equal(t, "acme-infra-dev.tfplan", p.StoredPlanFilePath())
}
