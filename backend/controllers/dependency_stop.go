package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// StopLegacyDependentApi is registered only under internal operator auth.
// Plan never writes; execute fences before a separate verify can settle history.
func (d DiggerController) StopLegacyDependentApi(c *gin.Context) {
	batchID, err := uuid.Parse(c.Param("batch_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch UUID required"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var request models.DependencyStopRequest
	if err = decoder.Decode(&request); err == nil {
		var extra interface{}
		if decoder.Decode(&extra) != io.EOF {
			err = models.ErrDependencyStopConflict
		}
	}
	if err != nil || request.Validate() != nil {
		code := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		c.JSON(code, gin.H{"error": "a bounded version-1 plan, execute or verify request is required"})
		return
	}
	actor := c.GetString(middleware.USER_ID_KEY)
	if strings.TrimSpace(actor) == "" || len(actor) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bounded operator attribution required"})
		return
	}
	var org models.Organisation
	if err = models.DB.GormDB.Where("external_id = ? AND external_source = ?", c.GetString(middleware.ORGANISATION_ID_KEY), c.GetString(middleware.ORGANISATION_SOURCE_KEY)).First(&org).Error; err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	repo, err := models.DB.GetRepoById(org.ID, c.Param("repo_id"))
	if err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	batch, err := models.DB.GetDiggerBatch(&batchID)
	if err != nil || batch.ID != batchID || batch.RepoFullName != repo.RepoFullName || batch.VCS != models.DiggerVCSGithub {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	var linked int64
	if err = models.DB.GormDB.Model(&models.GithubAppInstallationLink{}).Where("organisation_id = ? AND github_installation_id = ? AND status = ?", org.ID, batch.GithubInstallationId, models.GithubAppInstallationLinkActive).Count(&linked).Error; err != nil || linked == 0 {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	slog.Info("digger.dependency_stop.received", "operation_id", strconv.Quote(request.OperationID.String()), "batch_id", strconv.Quote(batchID.String()), "repo_id", repo.ID, "mode", strconv.Quote(request.Mode), "actor", strconv.Quote(actor))
	if request.Mode != "plan" {
		old, lookupErr := models.DB.GetDependencyStop(org.ID, repo.ID, batchID, request)
		if lookupErr == nil && request.Mode == "execute" {
			d.respondDependencyStop(c, old, "")
			return
		}
		if lookupErr != nil && !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			dependencyStopRejected(c, lookupErr)
			return
		}
		if request.Mode == "verify" && errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
	}
	// This gate admits new fences only after the operator has qualified every
	// replica/reader. Disabling it never releases or strands an existing fence.
	if request.Mode == "execute" && os.Getenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED") != "true" {
		slog.Info("digger.dependency_stop.disabled", "operation_id", strconv.Quote(request.OperationID.String()))
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "new historical stops are disabled pending reader cutover qualification"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	client, _, err := utils.GetGithubClient(d.GithubClientProvider, batch.GithubInstallationId, batch.RepoFullName)
	if err != nil {
		dependencyStopRejected(c, err)
		return
	}
	switch request.Mode {
	case "plan", "execute":
		plan, err := models.DB.PlanDependencyStop(org.ID, repo.ID, batchID, request)
		if err != nil {
			if request.Mode == "execute" {
				if existing, lookupErr := models.DB.GetDependencyStop(org.ID, repo.ID, batchID, request); lookupErr == nil {
					d.respondDependencyStop(c, existing, "")
					return
				}
			}
			dependencyStopRejected(c, err)
			return
		}
		if _, err = services.CheckDependencyStopSource(ctx, client, batch, request, plan.Snapshot.Origin.RepositoryID); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "original command, merged PR and named failed attempt could not be verified"})
			return
		}
		if request.Mode == "plan" {
			slog.Info("digger.dependency_stop.planned", "operation_id", strconv.Quote(request.OperationID.String()), "plan_sha256", strconv.Quote(plan.PlanSHA256))
			c.JSON(http.StatusOK, gin.H{"plan": plan, "execution_enabled": os.Getenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED") == "true"})
			return
		}
		stop, err := models.DB.FenceDependencyStop(org.ID, repo.ID, batchID, request, actor)
		if err != nil {
			dependencyStopRejected(c, err)
			return
		}
		d.respondDependencyStop(c, stop, "")
	case "verify":
		view, err := models.DB.ReadDependencyStopView(org.ID, repo.ID, batchID, request)
		if err != nil {
			dependencyStopRejected(c, err)
			return
		}
		evidence, err := services.VerifyDependencyStopCheckpoint(ctx, client, batch, view)
		if err != nil {
			reason := "provider_evidence_unavailable"
			if errors.Is(err, services.ErrDependencyStopActionsActive) {
				reason = "repository_actions_active"
			}
			d.respondDependencyStop(c, &view.Operation, reason)
			return
		}
		stop, err := models.DB.VerifyDependencyStop(org.ID, repo.ID, batchID, request, view.StateSHA256, actor, *evidence)
		if err != nil {
			d.respondDependencyStop(c, &view.Operation, "local_state_changed_or_verification_conflict")
			return
		}
		d.respondDependencyStop(c, stop, "")
	}
}

func dependencyStopRejected(c *gin.Context, err error) {
	slog.Warn("digger.dependency_stop.rejected", "reason", strconv.Quote(err.Error()))
	c.JSON(http.StatusConflict, gin.H{"error": "historical stop could not be safely accepted"})
}

func (d DiggerController) respondDependencyStop(c *gin.Context, stop *models.DependencyStop, pendingReason string) {
	batch, err := models.DB.GetDiggerBatch(&stop.BatchID)
	if err == nil {
		err = refreshDependencyRetirementReports(d.GithubClientProvider, batch)
	}
	if err == nil {
		err = UpdateCheckRunForBatch(d.GithubClientProvider, batch, false)
	}
	status := http.StatusOK
	if stop.CurrentCheckpoint == nil {
		status = http.StatusAccepted
	}
	body := gin.H{"operation": stop, "settlement_state": stop.Phase(), "prior_execution": "unknown"}
	if batch != nil {
		body["batch_status"] = batch.Status
	}
	if pendingReason != "" {
		body["pending_reason"] = pendingReason
	}
	if err != nil {
		status = http.StatusBadGateway
		body["error"] = "stop state persisted; retry the same operation to refresh existing reports"
	}
	slog.Info("digger.dependency_stop.decided", "operation_id", strconv.Quote(stop.OperationID.String()), "batch_id", strconv.Quote(stop.BatchID.String()), "settlement_state", strconv.Quote(stop.Phase()), "status", status)
	c.JSON(status, body)
}
