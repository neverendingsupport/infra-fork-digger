package controllers_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/controllers"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const pr51CommandOriginMigration = "20261030140000_batch_command_origins.sql"

// Each test creates and drops its own database. Set this URL to a disposable
// PostgreSQL 16 instance whose role can create databases; no default is assumed.
func pr51CommandOriginPostgresDB(t *testing.T, migrate bool) *models.Database {
	t.Helper()
	dsn := os.Getenv("DIGGER_COMMAND_ORIGIN_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DIGGER_COMMAND_ORIGIN_POSTGRES_DSN for PostgreSQL qualification")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, parsed.Scheme)
	query := parsed.Query()
	query.Set("statement_timeout", "10000")
	query.Set("lock_timeout", "5000")
	parsed.RawQuery = query.Encode()
	admin := pr51CommandOriginPostgresConnect(t, parsed.String())
	name := "digger41_origin_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec(`CREATE DATABASE "`+name+`"`).Error)
	t.Cleanup(func() {
		require.NoError(t, admin.Exec(`DROP DATABASE "`+name+`" WITH (FORCE)`).Error)
	})
	parsed.Path = "/" + name
	db := &models.Database{GormDB: pr51CommandOriginPostgresConnect(t, parsed.String())}
	files, err := filepath.Glob(filepath.Join("..", "migrations", "*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.True(t, slices.ContainsFunc(files, func(f string) bool { return filepath.Base(f) == pr51CommandOriginMigration }))
	for _, path := range files {
		if filepath.Base(path) == pr51CommandOriginMigration && !migrate {
			break
		}
		pr51CommandOriginApplySQL(t, db.GormDB, path)
	}
	return db
}

func pr51CommandOriginPostgresConnect(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return db
}

func pr51CommandOriginApplySQL(t *testing.T, db *gorm.DB, path string) {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return tx.Exec(string(body)).Error
	}), filepath.Base(path))
}

func pr51LegacyStopFixture(t *testing.T, db *models.Database, sibling bool) (models.DiggerBatch, models.Organisation, models.Repo, models.DiggerJob, models.DiggerJob, models.DependencyStopRequest) {
	t.Helper()
	org := models.Organisation{Model: gorm.Model{ID: 2}, Name: "neverendingsupport", ExternalId: "legacy-org", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&org).Error)
	repo := models.Repo{Model: gorm.Model{ID: 1}, Name: "infra-terraform", RepoFullName: "neverendingsupport/infra-terraform", OrganisationID: org.ID}
	require.NoError(t, db.GormDB.Create(&repo).Error)
	require.NoError(t, db.GormDB.Create(&models.GithubAppInstallationLink{OrganisationId: org.ID, GithubInstallationId: 171074811, Status: models.GithubAppInstallationLinkActive}).Error)
	check, comment := "129327714611", int64(6746869800)
	batch := models.DiggerBatch{ID: uuid.MustParse("4bddc990-19fe-4838-a267-b072df73aa82"), VCS: models.DiggerVCSGithub, RepoFullName: repo.RepoFullName, RepoOwner: "neverendingsupport", RepoName: "infra-terraform", PrNumber: 3197, CommitSha: "ecdc93f734c02e56cd6e0fbe6b324a6a0600b07a", GithubInstallationId: 171074811, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated, CheckRunId: &check, CommentId: &comment}
	require.NoError(t, db.GormDB.Create(&batch).Error)
	projects := []string{"access-control", "infra-k8s"}
	if sibling {
		projects = append(projects, "independent")
	}
	source := models.GitHubCommandSource{RepositoryID: 954216504, CommentID: 6746866728, BodySHA256: "ba7707804cf8a8400a1bac3f2d7f291966476b77974b7ca2979ecad1eda5cc02"}
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, source, projects))
	makeJob := func(id uint, jobID, project, check string, status scheduler.DiggerJobStatus, runID int64) models.DiggerJob {
		summary := models.DiggerJobSummary{}
		require.NoError(t, db.GormDB.Create(&summary).Error)
		spec, err := json.Marshal(scheduler.JobJson{ProjectName: project, Namespace: batch.RepoFullName, Commit: batch.CommitSha, PullRequestNumber: &batch.PrNumber, JobType: "apply"})
		require.NoError(t, err)
		batchID, url := batch.ID.String(), "#"
		attempt := int64(0)
		if runID > 0 {
			attempt = 1
			url = "https://github.com/neverendingsupport/infra-terraform/actions/runs/42665476673"
		}
		checkURL := "https://github.com/neverendingsupport/infra-terraform/runs/" + check
		job := models.DiggerJob{Model: gorm.Model{ID: id}, DiggerJobID: jobID, ProjectName: project, BatchID: &batchID, CheckRunId: &check, CheckRunUrl: &checkURL, SerializedJobSpec: spec, DiggerJobSummaryID: summary.ID, Status: status, WorkflowRunID: runID, WorkflowRunAttempt: attempt, WorkflowRunUrl: &url}
		require.NoError(t, db.GormDB.Create(&job).Error)
		return job
	}
	parent := makeJob(2752, "b2pIz8ZHrR", "access-control", "129327710180", scheduler.DiggerJobFailed, 42665476673)
	// The actual parent retained its dispatch claim. This says nothing about prior child execution.
	require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET workflow_dispatch_started_at=? WHERE id=?", time.Date(2026, 11, 9, 17, 31, 38, 101616000, time.UTC), parent.ID).Error)
	child := makeJob(2753, "wWyMzTVZcl", "infra-k8s", "129327703323", scheduler.DiggerJobCreated, 0)
	if sibling {
		makeJob(2754, "independent", "independent", "129327703324", scheduler.DiggerJobStarted, 42665476674)
	}
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	_, err := db.CreateDiggerJobLink(child.DiggerJobID, batch.RepoFullName)
	require.NoError(t, err)
	require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
	request := models.DependencyStopRequest{Version: 1, Mode: "plan", OperationID: uuid.MustParse("a06c8142-4143-422b-9e44-377766aa3197"), PRNumber: batch.PrNumber, SourceCommentID: source.CommentID, BodySHA256: source.BodySHA256, CommitSHA: batch.CommitSha, Targets: []models.DependencyStopJobRef{{child.ID, child.DiggerJobID}}, FailedPrerequisite: models.DependencyStopPrerequisite{DependencyStopJobRef: models.DependencyStopJobRef{parent.ID, parent.DiggerJobID}, WorkflowRunID: parent.WorkflowRunID, WorkflowRunAttempt: 1}, SettlementPolicy: models.DependencyStopPolicy}
	return batch, org, repo, parent, child, request
}

func pr51LegacyStopEvidence(stop models.DependencyStop) models.DependencyStopVerification {
	now := time.Now().UTC()
	counts := map[string]int{}
	for _, s := range models.DependencyStopIncompleteStatuses {
		counts[s] = 0
	}
	return models.DependencyStopVerification{CurrentParentRunID: stop.Request.FailedPrerequisite.WorkflowRunID, CurrentParentAttempt: 1, CurrentParentStatus: scheduler.DiggerJobFailed, CurrentParentProviderStatus: "completed", CurrentParentConclusion: "failure", Version: 1, RepositoryID: stop.Snapshot.Origin.RepositoryID, RepoFullName: stop.Snapshot.Origin.RepoFullName, PRNumber: stop.Request.PRNumber, Merged: true, PRState: "closed", PRHeadSHA: strings.Repeat("1", 40), MergeCommitSHA: strings.Repeat("2", 40), SourceCommentID: stop.Request.SourceCommentID, BodySHA256: stop.Request.BodySHA256, ParentRunID: stop.Request.FailedPrerequisite.WorkflowRunID, ParentAttempt: 1, ParentStatus: "completed", ParentConclusion: "failure", ActionsComplete: true, ActiveCounts: counts, StartedAt: now, FinishedAt: now}
}

type pr51Transport func(*http.Request) (*http.Response, error)

func (f pr51Transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func pr51Install(t *testing.T, db *models.Database) {
	previous := models.DB
	models.DB = db
	t.Cleanup(func() { models.DB = previous })
	_, err := db.CreateGithubAppInstallation(171074811, 1, "neverendingsupport", 1, "neverendingsupport/infra-terraform")
	require.NoError(t, err)
}

func pr51Fence(t *testing.T, db *models.Database, batch models.DiggerBatch, org models.Organisation, repo models.Repo, req models.DependencyStopRequest, verify bool) {
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	if verify {
		view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
		require.NoError(t, err)
		_, err = db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", pr51LegacyStopEvidence(*stop))
		require.NoError(t, err)
	}
}

func TestPR51CheckRejectsPreparedContentsAfterRerun(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		name := "job"
		if aggregate {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			db := pr51CommandOriginPostgresDB(t, true)
			batch, org, repo, parent, child, req := pr51LegacyStopFixture(t, db, false)
			pr51Install(t, db)
			pr51Fence(t, db, batch, org, repo, req, true)
			var payloads []map[string]interface{}
			provider := utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: pr51Transport(func(r *http.Request) (*http.Response, error) {
				if r.Method == "PATCH" {
					var body map[string]interface{}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					payloads = append(payloads, body)
				} else {
					require.Equal(t, "GET", r.Method)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":129327703323,"name":"digger/apply","output":{}}`)), Request: r}, nil
			})}}
			publish := func() error {
				b, err := db.GetDiggerBatch(&batch.ID)
				require.NoError(t, err)
				if aggregate {
					return controllers.UpdateCheckRunForBatch(provider, b, false)
				}
				j, err := db.GetDiggerJob(child.DiggerJobID)
				require.NoError(t, err)
				return controllers.UpdateCheckRunForJob(provider, j, false)
			}
			require.NoError(t, publish())
			require.Len(t, payloads, 1)
			require.Equal(t, "completed", payloads[0]["status"])
			if !aggregate {
				require.Equal(t, "skipped", payloads[0]["conclusion"])
			}
			payloads = nil
			counts := 0
			require.NoError(t, db.GormDB.Callback().Query().After("gorm:query").Register("pr51:rerun_before_barrier", func(tx *gorm.DB) {
				if tx.Statement.Table != "digger_jobs" || !strings.Contains(tx.Statement.SQL.String(), "count(") || !strings.Contains(tx.Statement.SQL.String(), "dependency_retirement_operation_id IS NOT NULL") {
					return
				}
				counts++
				// First count prepares controller content; second is PublishBatchCheck's preflight.
				if counts != 2 {
					return
				}
				next, err := db.GetDiggerJob(parent.DiggerJobID)
				require.NoError(t, err)
				next.Status = scheduler.DiggerJobStarted
				next.WorkflowRunAttempt = 2
				next.StatusUpdatedAt = time.Now().UTC()
				_, err = db.AcceptDiggerJobCallback(next, org.ID)
				require.NoError(t, err)
			}))
			err := publish()
			require.NoError(t, db.GormDB.Callback().Query().Remove("pr51:rerun_before_barrier"))
			require.GreaterOrEqual(t, counts, 2)
			t.Logf("prepared publication after rerun: error=%v provider payloads=%v", err, payloads)
			require.ErrorIs(t, err, models.ErrDependencyStopConflict)
			require.Empty(t, payloads, "stale completed contents must not reach the provider")
			require.NoError(t, publish())
			require.Len(t, payloads, 1)
			require.Equal(t, "in_progress", payloads[0]["status"])
			require.NotContains(t, payloads[0], "conclusion")
		})
	}
}

func TestPR51HistoricalPublicationPreservesCountsThroughWriters(t *testing.T) {
	db := pr51CommandOriginPostgresDB(t, true)
	batch, org, repo, parent, _, req := pr51LegacyStopFixture(t, db, true)
	pr51Install(t, db)
	sibling, err := db.GetDiggerJob("independent")
	require.NoError(t, err)
	for _, row := range []struct {
		id      uint
		c, u, d uint
	}{{parent.DiggerJobSummaryID, 7, 8, 9}, {sibling.DiggerJobSummaryID, 11, 12, 13}} {
		require.NoError(t, db.GormDB.Model(&models.DiggerJobSummary{}).Where("id=?", row.id).Updates(map[string]interface{}{"resources_created": row.c, "resources_updated": row.u, "resources_deleted": row.d}).Error)
	}
	pr51Fence(t, db, batch, org, repo, req, false)
	var reports []string
	var title string
	provider := utils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: pr51Transport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "PATCH" {
			var body struct {
				Body   string `json:"body"`
				Output struct {
					Title string `json:"title"`
				} `json:"output"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if body.Body != "" {
				reports = append(reports, body.Body)
			} else {
				title = body.Output.Title
			}
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":6746869800,"name":"digger/apply","output":{}}`)), Request: r}, nil
	})}}
	require.NoError(t, utils.UpdatePRCommentRealtime(provider, &batch))
	service, err := utils.GetPrServiceFromBatch(&batch, provider)
	require.NoError(t, err)
	_, err = utils.UpdateOrCreateSummaryComment(service, &batch, "stale caller content")
	require.NoError(t, err)
	require.Len(t, reports, 2)
	for _, report := range reports {
		require.Contains(t, report, "| 7 | 8 | 9|")
		require.Contains(t, report, "| 11 | 12 | 13|")
		require.Contains(t, report, "prior execution unknown")
	}
	saved, err := db.GetDiggerJob(parent.DiggerJobID)
	require.NoError(t, err)
	require.NoError(t, controllers.UpdateCheckRunForJob(provider, saved, false))
	require.Equal(t, "7 created 8 updated 9 deleted", title)
	savedSibling, err := db.GetDiggerJob(sibling.DiggerJobID)
	require.NoError(t, err)
	require.NoError(t, controllers.UpdateCheckRunForJob(provider, savedSibling, false))
	require.Equal(t, "11 created 12 updated 13 deleted", title)
}

func TestPR51ScheduleReadySiblingAtCapacityInFencedBatch(t *testing.T) {
	db := pr51CommandOriginPostgresDB(t, true)
	batch, org, repo, parent, child, req := pr51LegacyStopFixture(t, db, true)
	pr51Install(t, db)
	ready, err := db.GetDiggerJob("independent")
	require.NoError(t, err)
	require.NoError(t, db.GormDB.Model(&models.DiggerJob{}).Where("id=?", ready.ID).Updates(map[string]interface{}{"status": scheduler.DiggerJobCreated, "workflow_run_id": 0, "workflow_run_attempt": 0, "workflow_run_url": "#"}).Error)
	pr51Fence(t, db, batch, org, repo, req, false)
	next, err := db.GetDiggerJob(parent.DiggerJobID)
	require.NoError(t, err)
	next.Status = scheduler.DiggerJobStarted
	next.WorkflowRunAttempt = 2
	next.StatusUpdatedAt = time.Now().UTC()
	_, err = db.AcceptDiggerJobCallback(next, org.ID)
	require.NoError(t, err)
	previous := config.DiggerConfig.GetInt("max_concurrency_per_batch")
	config.DiggerConfig.Set("max_concurrency_per_batch", 1)
	t.Cleanup(func() { config.DiggerConfig.Set("max_concurrency_per_batch", previous) })
	active, err := db.CountActiveDiggerJobDispatches(batch.ID.String())
	require.NoError(t, err)
	require.Equal(t, int64(1), active)
	require.NoError(t, services.ScheduleJob(nil, batch.RepoFullName, batch.RepoOwner, batch.RepoName, &batch.ID, ready, nil))
	saved, err := db.GetDiggerJob(ready.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, scheduler.DiggerJobQueuedForRun, saved.Status)
	require.Equal(t, int64(1), saved.CallbackRevision)
	require.NoError(t, services.ScheduleJob(nil, batch.RepoFullName, batch.RepoOwner, batch.RepoName, &batch.ID, ready, nil))
	again, err := db.GetDiggerJob(ready.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, saved.CallbackRevision, again.CallbackRevision)
	require.NoError(t, services.ScheduleJob(nil, batch.RepoFullName, batch.RepoOwner, batch.RepoName, &batch.ID, &child, nil))
	claimed, err := db.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	held, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.NotNil(t, held.DependencyRetiredAt)
	require.Nil(t, held.WorkflowDispatchStartedAt)
}
