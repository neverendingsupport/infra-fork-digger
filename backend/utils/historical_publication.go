package utils

import (
	"fmt"
	"github.com/diggerhq/digger/backend/models"
	gh "github.com/diggerhq/digger/libs/ci/github"
	"github.com/google/go-github/v61/github"
	"log/slog"
)

func HistoricalPublication(batch *models.DiggerBatch) (*models.HistoricalPublication, error) {
	return historicalPublication(models.DB, batch)
}

func historicalPublication(db *models.Database, batch *models.DiggerBatch) (*models.HistoricalPublication, error) {
	if db == nil {
		return nil, nil
	}
	publication, err := db.HistoricalPublication(batch.ID)
	if err != nil {
		slog.Warn("digger.publication.rejected", "batch_id", batch.ID, "reason", "historical_identity_conflict")
		return nil, err
	}
	if publication != nil {
		for i := range publication.Jobs {
			id := publication.Jobs[i].CheckRunId
			if id == nil || !publication.Checks[*id] {
				return nil, models.ErrDependencyStopConflict
			}
			url := fmt.Sprintf("https://%s/%s/runs/%s", GetGithubHostname(), publication.Batch.RepoFullName, *id)
			publication.Jobs[i].CheckRunUrl = &url
		}
	}
	return publication, nil
}

// PublishBatchCheck is the common provider write boundary for batch/job checks.
func PublishBatchCheck(service *gh.GithubService, batch *models.DiggerBatch, id string, options gh.GithubCheckRunUpdateOptions) (*github.CheckRun, error) {

	// Preflight is a hint only. Reauthorize after acquiring the admission lock.
	if _, err := HistoricalPublication(batch); err != nil {
		return nil, err
	}
	if models.DB == nil {
		return service.UpdateCheckRun(id, options)
	}
	var result *github.CheckRun
	err := models.DB.WithPublicationBarrier(batch.ID, func(db *models.Database) error {
		publication, err := historicalPublication(db, batch)
		if err != nil {
			return err
		}
		if publication != nil && !publication.MatchesContent(batch) {
			return models.ErrDependencyStopConflict
		}
		if publication != nil && (service.Owner != publication.Batch.RepoOwner || service.RepoName != publication.Batch.RepoName || !publication.Checks[id] || batch.RepoFullName != publication.Batch.RepoFullName || batch.RepoOwner != publication.Batch.RepoOwner || batch.RepoName != publication.Batch.RepoName || batch.GithubInstallationId != publication.Batch.GithubInstallationId) {
			return models.ErrDependencyStopConflict
		}
		result, err = service.UpdateCheckRun(id, options)
		return err
	})
	return result, err
}
