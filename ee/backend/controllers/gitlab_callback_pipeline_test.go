package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/background"
	"github.com/diggerhq/digger/backend/ci_backends"
	backend "github.com/diggerhq/digger/backend/controllers"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type gitlabCallbackCI struct {
	t      *testing.T
	router *gin.Engine
	runs   []spec.Spec
}

func (p *gitlabCallbackCI) GetCiBackend(ci_backends.CiBackendOptions) (ci_backends.CiBackend, error) {
	return p, nil
}
func (p *gitlabCallbackCI) GetWorkflowUrl(spec.Spec) (string, error) { return "", nil }
func (p *gitlabCallbackCI) TriggerWorkflow(job spec.Spec, _, _ string) error {
	var owned int64
	if err := models.DB.GormDB.Model(&models.Repo{}).Where("repo_full_name = ? AND vcs = ?", job.VCS.RepoFullname, models.DiggerVCSGitlab).Count(&owned).Error; err != nil {
		return err
	}
	w := sendGitlabJobCallback(p.router, job, "started")
	p.t.Logf("registered GitLab started callback HTTP=%d; owned repositories before callback=%d", w.Code, owned)
	if w.Code != http.StatusOK {
		return fmt.Errorf("valid registered started callback returned %d: %s", w.Code, w.Body.String())
	}
	if owned != 1 {
		return fmt.Errorf("producer dispatched before persisting ownership")
	}
	p.runs = append(p.runs, job)
	return nil
}

func sendGitlabJobCallback(router *gin.Engine, job spec.Spec, status string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]interface{}{"status": status, "timestamp": time.Now().UTC(), "workflow_url": "https://gitlab.fixture/jobs/1"})
	req := httptest.NewRequest(http.MethodPost, "/repos/test/projects/dev/jobs/"+job.JobId+"/set-status", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+job.Job.BackendJobToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	background.Wait(ctx)
	return w
}

func TestGitlabRegisteredProducerPersistsOwnershipBeforeCallback(t *testing.T) {
	for _, scenario := range []string{"new", "legacy_placeholder", "ownership_failure", "wrong_secret"} {
		t.Run(scenario, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
			t.Setenv("DIGGER_GITLAB_WEBHOOK_SECRET", "fixture-secret")
			t.Setenv("DIGGER_GITLAB_ACCESS_TOKEN", "token")
			t.Setenv("DIGGER_REPO_ALLOW_LIST", "")
			gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "gitlab.db")), &gorm.Config{})
			require.NoError(t, err)
			require.NoError(t, gdb.AutoMigrate(&models.Organisation{}, &models.Repo{}, &models.DiggerBatch{}, &models.DiggerJob{}, &models.DiggerJobParentLink{}, &models.GithubDiggerJobLink{}, &models.JobToken{}, &models.GithubAppInstallation{}))
			require.NoError(t, gdb.Exec("CREATE UNIQUE INDEX gitlab_repo_identity ON repos(name, organisation_id)").Error)
			require.NoError(t, gdb.Migrator().DropIndex(&models.Repo{}, "idx_org_repo"))
			require.NoError(t, gdb.AutoMigrate(&models.ImpactedProject{}))
			previous := models.DB
			models.DB = &models.Database{GormDB: gdb}
			t.Cleanup(func() { models.DB = previous; sqlDB, _ := gdb.DB(); _ = sqlDB.Close() })
			org, err := models.DB.CreateOrganisation(models.DEFAULT_ORG_NAME, "", models.DEFAULT_ORG_NAME, nil)
			require.NoError(t, err)
			if scenario == "legacy_placeholder" {
				require.NoError(t, gdb.Create(&models.Repo{Name: "group-repo", OrganisationID: org.ID}).Error)
			}
			if scenario == "ownership_failure" {
				require.NoError(t, gdb.Callback().Create().Before("gorm:create").Register("test:ownership_failure", func(tx *gorm.DB) {
					if tx.Statement.Table == "repos" {
						tx.AddError(fmt.Errorf("injected ownership persistence failure"))
					}
				}))
			}
			config := "pr_locks: false\nprojects:\n- name: dev\n  dir: dev\n  workflow: default\n"
			checkout := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(checkout, "digger.yml"), []byte(config), 0600))
			require.NoError(t, os.Mkdir(filepath.Join(checkout, "dev"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(checkout, "dev/main.tf"), []byte("# fixture\n"), 0600))
			for _, args := range [][]string{{"init", "-b", "feature"}, {"add", "."}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", "fixture"}} {
				cmd := exec.Command("git", args...)
				cmd.Dir = checkout
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", out)
			}
			t.Setenv("GIT_CONFIG_COUNT", "1")
			t.Setenv("GIT_CONFIG_KEY_0", "url.file://"+checkout+".insteadOf")
			t.Setenv("GIT_CONFIG_VALUE_0", "https://x-access-token:token@fixture.invalid/gitlab.git")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/repository/files/digger.yml/raw"):
					fmt.Fprint(w, config)
				case strings.HasSuffix(r.URL.Path, "/diffs"):
					fmt.Fprint(w, `[{"new_path":"dev/main.tf"}]`)
				case strings.HasSuffix(r.URL.Path, "/discussions"):
					fmt.Fprint(w, `{"id":"discussion","notes":[{"id":10,"body":"report"}]}`)
				case strings.Contains(r.URL.Path, "/discussions/"):
					fmt.Fprint(w, `{"id":11,"body":"report"}`)
				case strings.HasSuffix(r.URL.Path, "/merge_requests/9"):
					fmt.Fprint(w, `{"iid":9,"source_branch":"feature","target_branch":"main","sha":"head"}`)
				default:
					t.Errorf("unexpected GitLab fixture request: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected fixture request", 500)
				}
			}))
			defer server.Close()
			t.Setenv("DIGGER_GITLAB_BASE_URL", server.URL+"/api/v4")
			router := gin.New()
			router.POST("/repos/:repo/projects/:projectName/jobs/:jobId/set-status", middleware.JWTBearerTokenAuth(services.Auth{}), middleware.AccessLevel(models.CliJobAccessType, models.AdminPolicyType), (backend.DiggerController{}).SetJobStatusForProject)
			ci := &gitlabCallbackCI{t: t, router: router}
			controller := DiggerEEController{GitlabProvider: utils.GitlabClientProvider{}, CiBackendProvider: ci}
			router.POST("/gitlab-webhook", controller.GitlabWebHookHandler)
			payload := `{"object_kind":"merge_request","event_type":"merge_request","user":{"username":"alice"},"project":{"id":1,"name":"repo","path_with_namespace":"group/repo","namespace":"group","git_http_url":"https://fixture.invalid/gitlab.git"},"repository":{"default_branch":"main"},"object_attributes":{"id":99,"iid":9,"source_branch":"feature","target_branch":"main","action":"open","last_commit":{"id":"head"}}}`
			req := httptest.NewRequest(http.MethodPost, "/gitlab-webhook", strings.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Gitlab-Event", "Merge Request Hook")
			secret := "fixture-secret"
			if scenario == "wrong_secret" {
				secret = "wrong"
			}
			req.Header.Set("X-Gitlab-Token", secret)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if scenario == "ownership_failure" || scenario == "wrong_secret" {
				expected := http.StatusInternalServerError
				if scenario == "wrong_secret" {
					expected = http.StatusBadRequest
				}
				require.Equal(t, expected, w.Code, w.Body.String())
				require.Empty(t, ci.runs)
				for _, model := range []interface{}{&models.JobToken{}, &models.DiggerJob{}, &models.DiggerBatch{}} {
					var count int64
					require.NoError(t, gdb.Model(model).Count(&count).Error)
					require.Zero(t, count)
				}
				return
			}
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, ci.runs, 1)
			finished := sendGitlabJobCallback(router, ci.runs[0], "failed")
			require.Equal(t, http.StatusOK, finished.Code, finished.Body.String())
			job, err := models.DB.GetDiggerJob(ci.runs[0].JobId)
			require.NoError(t, err)
			require.Equal(t, scheduler.DiggerJobFailed, job.Status)
			require.Zero(t, job.DispatchProtocolVersion)
			var owned models.Repo
			require.NoError(t, gdb.Where("repo_full_name = ? AND organisation_id = ?", "group/repo", org.ID).First(&owned).Error)
			require.Equal(t, models.DiggerVCSGitlab, owned.VCS)
		})
	}
}
