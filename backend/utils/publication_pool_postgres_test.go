package utils

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type publicationPoolTransport func(*http.Request) (*http.Response, error)

func (f publicationPoolTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPublicationCanonicalCommentWriterOneConnection(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		name := "ordinary"
		if fenced {
			name = "historical"
		}
		t.Run(name, func(t *testing.T) {
			dsn := os.Getenv("DIGGER_COMMAND_ORIGIN_POSTGRES_DSN")
			if dsn == "" {
				t.Skip("set local disposable PostgreSQL DSN")
			}
			u, err := url.Parse(dsn)
			require.NoError(t, err)
			connect := func(dsn string) *gorm.DB {
				db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
				require.NoError(t, err)
				return db
			}
			admin := connect(dsn)
			adminPool, err := admin.DB()
			require.NoError(t, err)
			defer adminPool.Close()
			dbName := "digger_publication_pool_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, admin.Exec(`CREATE DATABASE "`+dbName+`"`).Error)
			defer func() { require.NoError(t, admin.Exec(`DROP DATABASE "`+dbName+`" WITH (FORCE)`).Error) }()
			u.Path = "/" + dbName
			orm := connect(u.String())
			pool, err := orm.DB()
			require.NoError(t, err)
			defer pool.Close()
			migrations, err := filepath.Glob("../migrations/*.sql")
			require.NoError(t, err)
			for _, path := range migrations {
				sql, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, orm.Exec(string(sql)).Error, filepath.Base(path))
			}
			db := &models.Database{GormDB: orm}
			previous := models.DB
			models.DB = db
			defer func() { models.DB = previous }()
			org := models.Organisation{Name: "pool", ExternalId: "pool", ExternalSource: "test"}
			require.NoError(t, orm.Create(&org).Error)
			repo := models.Repo{Name: "repo", RepoFullName: "owner/repo", OrganisationID: org.ID}
			require.NoError(t, orm.Create(&repo).Error)
			_, err = db.CreateGithubInstallationLink(&org, 77)
			require.NoError(t, err)
			_, err = db.CreateGithubAppInstallation(77, 1, "owner", 1, "owner/repo")
			require.NoError(t, err)
			report, check := int64(9000), "1000"
			batch := models.DiggerBatch{ID: uuid.New(), VCS: models.DiggerVCSGithub, RepoFullName: "owner/repo", RepoOwner: "owner", RepoName: "repo", PrNumber: 3197, CommitSha: strings.Repeat("a", 40), GithubInstallationId: 77, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated, CommentId: &report, CheckRunId: &check}
			require.NoError(t, orm.Create(&batch).Error)
			source := models.GitHubCommandSource{RepositoryID: 123, CommentID: 456, BodySHA256: strings.Repeat("b", 64)}
			require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, source, []string{"parent", "child"}))
			makeJob := func(project, check string, status scheduler.DiggerJobStatus, run int64) models.DiggerJob {
				summary := models.DiggerJobSummary{}
				require.NoError(t, orm.Create(&summary).Error)
				spec, err := json.Marshal(scheduler.JobJson{ProjectName: project, Namespace: batch.RepoFullName, PullRequestNumber: &batch.PrNumber, Commit: batch.CommitSha, JobType: "apply", Commands: []string{"digger apply"}})
				require.NoError(t, err)
				batchID, url := batch.ID.String(), "#"
				attempt := int64(0)
				if run > 0 {
					attempt = 1
					url = "https://github.com/owner/repo/actions/runs/101"
				}
				checkURL := "https://github.com/owner/repo/runs/" + check
				job := models.DiggerJob{DiggerJobID: project, ProjectName: project, BatchID: &batchID, CheckRunId: &check, CheckRunUrl: &checkURL, DiggerJobSummaryID: summary.ID, SerializedJobSpec: spec, Status: status, WorkflowRunID: run, WorkflowRunAttempt: attempt, WorkflowRunUrl: &url}
				require.NoError(t, orm.Create(&job).Error)
				return job
			}
			parent := makeJob("parent", "1001", scheduler.DiggerJobFailed, 101)
			child := makeJob("child", "1002", scheduler.DiggerJobCreated, 0)
			require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
			_, err = db.CreateDiggerJobLink(child.DiggerJobID, batch.RepoFullName)
			require.NoError(t, err)
			require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
			if fenced {
				req := models.DependencyStopRequest{Version: 1, Mode: "plan", OperationID: uuid.New(), PRNumber: batch.PrNumber, SourceCommentID: source.CommentID, BodySHA256: source.BodySHA256, CommitSHA: batch.CommitSha, Targets: []models.DependencyStopJobRef{{DBID: child.ID, JobID: child.DiggerJobID}}, FailedPrerequisite: models.DependencyStopPrerequisite{DependencyStopJobRef: models.DependencyStopJobRef{DBID: parent.ID, JobID: parent.DiggerJobID}, WorkflowRunID: 101, WorkflowRunAttempt: 1}, SettlementPolicy: models.DependencyStopPolicy}
				plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
				require.NoError(t, err)
				req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
				_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
				require.NoError(t, err)
			}
			pool.SetMaxOpenConns(1)
			pool.SetMaxIdleConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			models.DB = &models.Database{GormDB: orm.WithContext(ctx)}
			writes := 0
			provider := DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: publicationPoolTransport(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, "PATCH", r.Method)
				require.Equal(t, "/repos/owner/repo/issues/comments/9000", r.URL.Path)
				writes++
				var body struct {
					Body string `json:"body"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Contains(t, body.Body, "**child**")
				if fenced {
					require.Contains(t, body.Body, "prior execution unknown")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":9000}`)), Request: r}, nil
			})}}
			before := pool.Stats().WaitCount
			require.NoError(t, UpdatePRCommentRealtime(provider, &batch), "the actual canonical writer must resolve installation without a second connection")
			require.Equal(t, 1, writes)
			require.Equal(t, before, pool.Stats().WaitCount)
			require.Equal(t, 1, pool.Stats().MaxOpenConnections)
		})
	}
}
