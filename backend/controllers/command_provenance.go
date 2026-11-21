package controllers

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
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
	// The receipt is a sibling object, so v1 readers that check keys strictly
	// see an unchanged response unless they ask for it.
	withRejection := slices.Equal(query["include"], []string{"command_rejection"})
	if query.Has("include") && !withRejection {
		c.JSON(http.StatusBadRequest, gin.H{"error": "include must be command_rejection"})
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
	response := gin.H{"repo": repo.MapToJsonStruct(), "command_provenance": provenance}
	if withRejection {
		rejection, err := models.DB.GetCommandRejection(orgID, repo.RepoFullName, int(pr), commentID)
		if err != nil {
			slog.Error("Could not read command rejection", "orgId", orgID,
				"repoFullName", repo.RepoFullName, "prNumber", pr, "commentId", commentID, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read command rejection"})
			return
		}
		response["command_rejection"] = rejection
	}
	slog.Info("Read command provenance", "orgId", orgID, "repoFullName", repo.RepoFullName,
		"prNumber", pr, "commentId", commentID, "withRejection", withRejection)
	c.JSON(http.StatusOK, response)
}
