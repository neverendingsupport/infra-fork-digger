package controllers

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/diggerhq/digger/backend/logging"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ReconcileBatchDependenciesApi is an idempotent repair, never a dispatch retry.
// The internal bearer credential is required by the registered API group.
func (d DiggerController) ReconcileBatchDependenciesApi(c *gin.Context) {
	var request struct {
		PrNumber        int    `json:"pr_number"`
		SourceCommentID int64  `json:"source_comment_id"`
		CommitSHA       string `json:"commit_sha"`
	}
	batchID, err := uuid.Parse(c.Param("batch_id"))
	if err != nil || c.ShouldBindJSON(&request) != nil || request.PrNumber <= 0 || request.SourceCommentID <= 0 || request.CommitSHA == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch UUID, pr_number, source_comment_id and commit_sha are required"})
		return
	}
	var org models.Organisation
	err = models.DB.GormDB.Where("external_id = ? AND external_source = ?", c.GetString(middleware.ORGANISATION_ID_KEY), c.GetString(middleware.ORGANISATION_SOURCE_KEY)).First(&org).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "organisation not found"})
		return
	}
	repo, err := models.DB.GetRepoById(org.ID, c.Param("repo_id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "repository not found"})
		return
	}
	slog.Info("digger.dependencies.recovery_received", "batch_id", batchID, "repo_id", repo.ID, "source_comment_id", logging.SafeLogValue(strconv.FormatInt(request.SourceCommentID, 10)), "actor", logging.SafeLogValue(c.GetString(middleware.USER_ID_KEY)))
	result, err := models.DB.ReconcileCommandDependencies(org.ID, repo.RepoFullName, request.PrNumber, request.SourceCommentID, request.CommitSHA, batchID)
	if err != nil {
		slog.Warn("digger.dependencies.recovery_rejected", "batch_id", batchID, "error", err)
		c.JSON(http.StatusConflict, gin.H{"error": "batch could not be safely reconciled"})
		return
	}
	batch, err := models.DB.GetDiggerBatch(&batchID)
	if err == nil {
		err = refreshDependencyRetirementReports(d.GithubClientProvider, batch)
	}
	if err == nil {
		err = UpdateCheckRunForBatch(d.GithubClientProvider, batch, false)
	}
	if err != nil {
		// Persistence precedes reports. Repeating the same operation repairs
		// display failures without releasing a fence or sending a workflow.
		slog.Warn("digger.dependencies.recovery_reporting_failed", "batch_id", batchID, "error", err)
		c.JSON(http.StatusBadGateway, gin.H{"reconciliation": result, "error": "reconciliation persisted; retry to refresh reports"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"reconciliation": result})
}

func refreshDependencyRetirementReports(gh utils.GithubClientProvider, batch *models.DiggerBatch) error {
	jobs, err := models.DB.GetDiggerJobsForBatch(batch.ID)
	if err != nil {
		return err
	}
	var failures []error
	anyRetired := false
	for i := range jobs {
		if jobs[i].DependencyRetiredAt != nil {
			anyRetired = true
			if err := UpdateCheckRunForJob(gh, &jobs[i], false); err != nil {
				failures = append(failures, err)
			}
		}
	}
	if anyRetired {
		failures = append(failures, utils.UpdatePRCommentRealtime(gh, batch))
	}
	return errors.Join(failures...)
}
