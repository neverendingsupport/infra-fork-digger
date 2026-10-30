package controllers

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/diggerhq/digger/backend/models"
	"github.com/gin-gonic/gin"
)

// getCommandProvenance is the bounded opt-in projection of the existing jobs GET.
// Repository ownership has already been checked by GetJobsForRepoApi.
func getCommandProvenance(c *gin.Context, orgID uint, repo *models.Repo) {
	query := c.Request.URL.Query()
	if len(query["source_comment_id"]) != 1 || len(query["pr_number"]) != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "one source_comment_id and pr_number are required"})
		return
	}
	commentID, commentErr := strconv.ParseInt(query.Get("source_comment_id"), 10, 64)
	pr, prErr := strconv.ParseInt(query.Get("pr_number"), 10, 32)
	if commentErr != nil || prErr != nil || commentID <= 0 || pr <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_comment_id and pr_number must be positive integers"})
		return
	}
	provenance, err := models.DB.GetCommandProvenance(orgID, repo.RepoFullName, int(pr), commentID)
	if errors.Is(err, models.ErrProvenanceTooLarge) {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "command execution history exceeds the evidence limit"})
		return
	}
	if err != nil {
		slog.Error("Could not read command provenance", "orgId", orgID,
			"repoFullName", repo.RepoFullName, "prNumber", pr, "commentId", commentID,
			"error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read command execution history"})
		return
	}
	slog.Info("Read command provenance", "orgId", orgID, "repoFullName", repo.RepoFullName,
		"prNumber", pr, "commentId", commentID)
	c.JSON(http.StatusOK, gin.H{"repo": repo.MapToJsonStruct(), "command_provenance": provenance})
}
