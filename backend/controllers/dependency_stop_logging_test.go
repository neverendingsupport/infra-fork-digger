package controllers

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStopLegacyDependentApiEscapesUntrustedLogValues(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("DIGGER_INTERNAL_SECRET", "test-internal-secret")
	t.Setenv("DIGGER_LEGACY_DEPENDENCY_STOP_ENABLED", "false")
	event := commandOriginEvent()
	event.Repo.FullName = github.String("owner/repo")
	event.Installation = &github.Installation{ID: github.Int64(77)}
	db, org, repo := commandPipelineDB(t, event)
	require.NoError(t, db.GormDB.AutoMigrate(&models.DependencyStop{}, &models.DependencyStopCheckpoint{}))
	batch := models.DiggerBatch{
		ID: uuid.New(), VCS: models.DiggerVCSGithub, RepoFullName: repo.RepoFullName,
		RepoOwner: "owner", RepoName: "repo", PrNumber: 9, GithubInstallationId: 77,
		BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated,
		CommitSha: strings.Repeat("a", 40),
	}
	require.NoError(t, db.GormDB.Create(&batch).Error)

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	controller := DiggerController{}
	router := gin.New()
	router.POST("/api/repos/:repo_id/batches/:batch_id/retire-failed-dependents",
		middleware.InternalApiAuth(), middleware.HeadersApiAuth(), controller.StopLegacyDependentApi)
	request := models.DependencyStopRequest{
		Version: 1, Mode: "execute", OperationID: uuid.New(), PRNumber: 9,
		SourceCommentID: 123, BodySHA256: strings.Repeat("b", 64),
		CommitSHA: batch.CommitSha,
		Targets:   []models.DependencyStopJobRef{{DBID: 1, JobID: "target"}},
		FailedPrerequisite: models.DependencyStopPrerequisite{
			DependencyStopJobRef: models.DependencyStopJobRef{DBID: 2, JobID: "prerequisite"},
			WorkflowRunID:        456, WorkflowRunAttempt: 1,
		},
		SettlementPolicy:   models.DependencyStopPolicy,
		ExpectedPlanSHA256: strings.Repeat("c", 64),
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)
	actor := "operator\r\nforged=record\t\x00" + strings.Repeat("x", 94)
	if len(actor) > 128 {
		t.Fatalf("test actor exceeds the handler's accepted length: %d", len(actor))
	}
	httpRequest := httptest.NewRequest(http.MethodPost,
		"/api/repos/"+strconv.FormatUint(uint64(repo.ID), 10)+"/batches/"+batch.ID.String()+"/retire-failed-dependents",
		bytes.NewReader(body))
	httpRequest.Header.Set("Authorization", "Bearer test-internal-secret")
	httpRequest.Header.Set("DIGGER_ORG_ID", org.ExternalId)
	httpRequest.Header.Set("DIGGER_ORG_SOURCE", org.ExternalSource)
	httpRequest.Header.Set("DIGGER_USER_ID", actor)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	var received map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &received))
	require.Equal(t, "digger.dependency_stop.received", received["msg"])
	encodedActor, ok := received["actor"].(string)
	require.True(t, ok)
	decodedActor, err := strconv.Unquote(encodedActor)
	require.NoError(t, err)
	require.Equal(t, actor, decodedActor)
	require.NotContains(t, lines[0], "\r")
	require.NotContains(t, lines[0], "forged=record\t")

	var disabled map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &disabled))
	require.Equal(t, "digger.dependency_stop.disabled", disabled["msg"])
	encodedOperationID, ok := disabled["operation_id"].(string)
	require.True(t, ok)
	decodedOperationID, err := strconv.Unquote(encodedOperationID)
	require.NoError(t, err)
	require.Equal(t, request.OperationID.String(), decodedOperationID)
}
