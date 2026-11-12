package models

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// These are the captured legacy #48 identities, not live database access.
func legacyStopFixture(t *testing.T, db *Database, sibling bool) (DiggerBatch, Organisation, Repo, DiggerJob, DiggerJob, DependencyStopRequest) {
	t.Helper()
	org := Organisation{Model: gorm.Model{ID: 2}, Name: "neverendingsupport", ExternalId: "legacy-org", ExternalSource: "test"}
	require.NoError(t, db.GormDB.Create(&org).Error)
	repo := Repo{Model: gorm.Model{ID: 1}, Name: "infra-terraform", RepoFullName: "neverendingsupport/infra-terraform", OrganisationID: org.ID}
	require.NoError(t, db.GormDB.Create(&repo).Error)
	require.NoError(t, db.GormDB.Create(&GithubAppInstallationLink{OrganisationId: org.ID, GithubInstallationId: 171074811, Status: GithubAppInstallationLinkActive}).Error)
	check, comment := "129327714611", int64(6746869800)
	batch := DiggerBatch{ID: uuid.MustParse("4bddc990-19fe-4838-a267-b072df73aa82"), VCS: DiggerVCSGithub, RepoFullName: repo.RepoFullName, RepoOwner: "neverendingsupport", RepoName: "infra-terraform", PrNumber: 3197, CommitSha: "ecdc93f734c02e56cd6e0fbe6b324a6a0600b07a", GithubInstallationId: 171074811, BatchType: scheduler.DiggerCommandApply, Status: scheduler.BatchJobCreated, CheckRunId: &check, CommentId: &comment}
	require.NoError(t, db.GormDB.Create(&batch).Error)
	projects := []string{"access-control", "infra-k8s"}
	if sibling {
		projects = append(projects, "independent")
	}
	source := GitHubCommandSource{RepositoryID: 954216504, CommentID: 6746866728, BodySHA256: "ba7707804cf8a8400a1bac3f2d7f291966476b77974b7ca2979ecad1eda5cc02"}
	require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, source, projects))
	makeJob := func(id uint, jobID, project, check string, status scheduler.DiggerJobStatus, runID int64) DiggerJob {
		summary := DiggerJobSummary{}
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
		job := DiggerJob{Model: gorm.Model{ID: id}, DiggerJobID: jobID, ProjectName: project, BatchID: &batchID, CheckRunId: &check, CheckRunUrl: &checkURL, SerializedJobSpec: spec, DiggerJobSummaryID: summary.ID, Status: status, WorkflowRunID: runID, WorkflowRunAttempt: attempt, WorkflowRunUrl: &url}
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
	request := DependencyStopRequest{Version: 1, Mode: "plan", OperationID: uuid.MustParse("a06c8142-4143-422b-9e44-377766aa3197"), PRNumber: batch.PrNumber, SourceCommentID: source.CommentID, BodySHA256: source.BodySHA256, CommitSHA: batch.CommitSha, Targets: []DependencyStopJobRef{{child.ID, child.DiggerJobID}}, FailedPrerequisite: DependencyStopPrerequisite{DependencyStopJobRef: DependencyStopJobRef{parent.ID, parent.DiggerJobID}, WorkflowRunID: parent.WorkflowRunID, WorkflowRunAttempt: 1}, SettlementPolicy: DependencyStopPolicy}
	return batch, org, repo, parent, child, request
}

func legacyStopEvidence(stop DependencyStop) DependencyStopVerification {
	now := time.Now().UTC()
	counts := map[string]int{}
	for _, s := range DependencyStopIncompleteStatuses {
		counts[s] = 0
	}
	return DependencyStopVerification{CurrentParentRunID: stop.Request.FailedPrerequisite.WorkflowRunID, CurrentParentAttempt: 1, CurrentParentStatus: scheduler.DiggerJobFailed, CurrentParentProviderStatus: "completed", CurrentParentConclusion: "failure", Version: 1, RepositoryID: stop.Snapshot.Origin.RepositoryID, RepoFullName: stop.Snapshot.Origin.RepoFullName, PRNumber: stop.Request.PRNumber, Merged: true, PRState: "closed", PRHeadSHA: strings.Repeat("1", 40), MergeCommitSHA: strings.Repeat("2", 40), SourceCommentID: stop.Request.SourceCommentID, BodySHA256: stop.Request.BodySHA256, ParentRunID: stop.Request.FailedPrerequisite.WorkflowRunID, ParentAttempt: 1, ParentStatus: "completed", ParentConclusion: "failure", ActionsComplete: true, ActiveCounts: counts, StartedAt: now, FinishedAt: now}
}

func TestLegacyStopPostgresPlanFenceVerifyRestart(t *testing.T) {
	for _, sibling := range []bool{false, true} {
		t.Run(map[bool]string{false: "actual_two_jobs", true: "running_sibling"}[sibling], func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			batch, org, repo, _, child, req := legacyStopFixture(t, db, sibling)
			plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
			require.NoError(t, err)
			var count int64
			require.NoError(t, db.GormDB.Model(&DependencyStop{}).Count(&count).Error)
			require.Zero(t, count)
			require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
			stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
			require.NoError(t, err)
			require.Equal(t, "unknown", stop.PriorExecution)
			require.Equal(t, "fenced_pending_verification", stop.Phase())
			// A restart and retry reuse the exact durable operation; no second fence.
			restarted, _ := commandOriginPostgresActor(t, db)
			retry, err := restarted.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "second-operator")
			require.NoError(t, err)
			require.True(t, stop.FencedAt.Equal(retry.FencedAt))
			require.Equal(t, "operator", retry.RequestedBy)
			saved, err := restarted.GetDiggerJob(child.DiggerJobID)
			require.NoError(t, err)
			require.True(t, saved.DependencyStopPending())
			require.Zero(t, saved.DispatchProtocolVersion)
			status, err := GetCheckRunStatusForJob(saved)
			require.NoError(t, err)
			require.Equal(t, "in_progress", status)
			conclusion, err := GetCheckRunConclusionForJob(saved)
			require.NoError(t, err)
			require.Empty(t, conclusion)
			require.NotContains(t, saved.DependencySkipReason(), "never dispatched")
			claimed, err := restarted.ClaimDiggerJobDispatch(&child)
			require.NoError(t, err)
			require.False(t, claimed)
			observed, err := restarted.ObserveDiggerJobCallback(&child)
			require.NoError(t, err)
			require.False(t, observed)
			require.ErrorIs(t, restarted.UpdateDiggerJob(&child), ErrDiggerJobRetired)
			result, err := restarted.ReconcileFailedDependencies(batch.ID)
			require.NoError(t, err)
			require.Equal(t, scheduler.BatchJobStarted, result.BatchStatus)
			require.Error(t, db.GormDB.Model(&DiggerBatch{}).Where("id = ?", batch.ID).Update("status", scheduler.BatchJobFailed).Error, "previous producer must not complete a pending audit")
			assertOutcome := func(verified bool) {
				p, err := restarted.GetCommandProvenance(org.ID, batch.RepoFullName, batch.PrNumber, req.SourceCommentID)
				require.NoError(t, err)
				require.True(t, p.Complete)
				require.True(t, p.Batches[0].ManifestComplete)
				var outcome *ExecutionOutcome
				for _, j := range p.Batches[0].Jobs {
					if j.JobID == child.DiggerJobID {
						outcome = j.ExecutionOutcome
						require.Zero(t, j.WorkflowRunID)
					}
				}
				require.NotNil(t, outcome)
				require.Equal(t, "future_dispatch_fenced", outcome.Kind)
				require.Equal(t, "unknown", outcome.PriorExecution)
				require.Equal(t, req.OperationID.String(), outcome.FenceID)
				require.Equal(t, outcome.FenceID, outcome.RecoveryID)
				require.Equal(t, plan.PlanSHA256, outcome.PlanSHA256)
				require.Equal(t, child.ID, outcome.Binding.Target.DBID)
				require.Equal(t, int64(42665476673), outcome.Binding.Prerequisite.WorkflowRunID)
				require.Equal(t, 2752, int(outcome.Binding.Manifest[0].DBID))
				require.Equal(t, batch.CommitSha, outcome.Binding.CommitSHA)
				data, err := json.Marshal(outcome)
				require.NoError(t, err)
				if verified {
					require.NotNil(t, outcome.VerifiedAt)
					require.NotNil(t, outcome.Settlement)
					require.Equal(t, strings.Repeat("1", 40), outcome.Settlement.PRHeadSHA)
					require.Equal(t, "verified", outcome.SettlementState)
				} else {
					require.Nil(t, outcome.VerifiedAt)
					require.NotContains(t, string(data), `"settlement":`)
					require.NotContains(t, string(data), `"verified_at":`)
				}
			}
			assertOutcome(false)
			req.Mode = "verify"
			view, err := restarted.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
			require.NoError(t, err)
			evidence := legacyStopEvidence(*stop)
			bad := evidence
			bad.Merged = false
			_, err = restarted.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", bad)
			require.ErrorIs(t, err, ErrDependencyStopConflict)
			verified, err := restarted.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", evidence)
			require.NoError(t, err)
			require.Equal(t, "unknown", verified.PriorExecution)
			assertOutcome(true)
			result, err = restarted.ReconcileFailedDependencies(batch.ID)
			require.NoError(t, err)
			if sibling {
				require.Equal(t, scheduler.BatchJobStarted, result.BatchStatus)
				require.Equal(t, scheduler.DiggerJobStarted, dependencySavedJob(t, db, 2754).Status)
				require.Error(t, db.GormDB.Model(&DiggerBatch{}).Where("id=?", batch.ID).Update("status", scheduler.BatchJobFailed).Error, "a verified target cannot terminalize an independent running sibling")
			} else {
				require.Equal(t, scheduler.BatchJobFailed, result.BatchStatus)
			}
			require.Error(t, db.GormDB.Model(&DependencyStop{}).Where("operation_id = ?", req.OperationID).Update("prior_execution", "never_dispatched").Error)
			require.Error(t, db.GormDB.Where("operation_id = ?", req.OperationID).Delete(&DependencyStop{}).Error)
			require.Error(t, db.GormDB.Model(&DependencyStop{}).Where("operation_id = ?", req.OperationID).Update("verified_at", time.Now()).Error)
			require.Error(t, db.GormDB.Exec("UPDATE digger_jobs SET dependency_retirement_operation_id=NULL WHERE id=?", child.ID).Error)
			require.Error(t, db.GormDB.Exec("UPDATE digger_jobs SET status=2,workflow_run_id=999 WHERE id=?", child.ID).Error)
		})
	}
}

func TestLegacyStopPostgresRejectsChangedPlanOrExecutionEvidence(t *testing.T) {
	for _, change := range []string{"claim", "observed", "attribution", "parent_attempt", "target_revision", "manifest", "wrong_org", "wrong_digest"} {
		t.Run(change, func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			batch, org, repo, parent, child, req := legacyStopFixture(t, db, false)
			plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
			require.NoError(t, err)
			req.Mode = "execute"
			req.ExpectedPlanSHA256 = plan.PlanSHA256
			switch change {
			case "claim", "observed":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET workflow_dispatch_started_at=? WHERE id=?", time.Now(), child.ID).Error)
			case "attribution":
				require.NoError(t, db.GormDB.Model(&GithubDiggerJobLink{}).Where("digger_job_id=?", child.DiggerJobID).Update("github_workflow_run_id", 999).Error)
			case "parent_attempt":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET workflow_run_attempt=2 WHERE id=?", parent.ID).Error)
			case "target_revision":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET callback_revision=1 WHERE id=?", child.ID).Error)
			case "manifest":
				require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET project_name='changed' WHERE id=?", child.ID).Error)
			case "wrong_org":
				org.ID++
			case "wrong_digest":
				req.ExpectedPlanSHA256 = strings.Repeat("0", 64)
			}
			_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
			require.Error(t, err)
			require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			var count int64
			require.NoError(t, db.GormDB.Model(&DependencyStop{}).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestLegacyStopPostgresObservationFenceRace(t *testing.T) {
	for _, winner := range []string{"fence", "runner_observation"} {
		t.Run(winner, func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			batch, org, repo, _, child, req := legacyStopFixture(t, db, false)
			plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
			require.NoError(t, err)
			req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
			lock := db.GormDB.Begin()
			require.NoError(t, lock.Error)
			t.Cleanup(func() { _ = lock.Rollback().Error })
			_, err = lockDependencyGraph(lock, batch.ID)
			require.NoError(t, err)
			var holder int
			require.NoError(t, lock.Raw("SELECT pg_backend_pid()").Scan(&holder).Error)
			actor, pid := commandOriginPostgresActor(t, db)
			if winner == "fence" {
				outcomes := make(chan bool, 1)
				failures := make(chan error, 1)
				go func() { ok, err := actor.ObserveDiggerJobCallback(&child); outcomes <- ok; failures <- err }()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				_, err = (&Database{GormDB: lock}).FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
				require.NoError(t, err)
				require.NoError(t, lock.Commit().Error)
				select {
				case ok := <-outcomes:
					require.False(t, ok)
				case <-time.After(8 * time.Second):
					t.Fatal("runner observation remained blocked")
				}
				require.NoError(t, <-failures)
			} else {
				outcomes := make(chan error, 1)
				go func() {
					_, err := actor.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
					outcomes <- err
				}()
				commandOriginPostgresWaitForBlocker(t, db, pid, holder)
				ok, err := (&Database{GormDB: lock}).ObserveDiggerJobCallback(&child)
				require.NoError(t, err)
				require.True(t, ok)
				require.NoError(t, lock.Commit().Error)
				select {
				case err := <-outcomes:
					require.ErrorIs(t, err, ErrDependencyStopConflict)
				case <-time.After(8 * time.Second):
					t.Fatal("operator remained blocked")
				}
				require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			}
		})
	}
}

func TestLegacyStopPostgresConcurrentIdempotentOperation(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, _, child, req := legacyStopFixture(t, db, false)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	first, _ := commandOriginPostgresActor(t, db)
	second, _ := commandOriginPostgresActor(t, db)
	type result struct {
		Stop *DependencyStop
		Err  error
	}
	results := make(chan result, 2)
	for _, actor := range []*Database{first, second} {
		go func(actor *Database) {
			s, e := actor.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
			results <- result{s, e}
		}(actor)
	}
	a, b := <-results, <-results
	require.NoError(t, a.Err)
	require.NoError(t, b.Err)
	require.True(t, a.Stop.FencedAt.Equal(b.Stop.FencedAt))
	var count int64
	require.NoError(t, db.GormDB.Model(&DependencyStop{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
	replacement := req
	replacement.OperationID = uuid.New()
	_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, replacement, "operator")
	require.ErrorIs(t, err, ErrDependencyStopConflict)
	require.Equal(t, req.OperationID, *dependencySavedJob(t, db, child.ID).DependencyRetirementOperationID)
}

func TestLegacyStopPostgresAtomicFenceFailures(t *testing.T) {
	for _, phase := range []string{"target_fence", "batch_pending"} {
		t.Run(phase, func(t *testing.T) {
			db := commandOriginPostgresDB(t, true)
			batch, org, repo, _, child, req := legacyStopFixture(t, db, false)
			plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
			require.NoError(t, err)
			req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
			table, condition := "digger_jobs", "NEW.dependency_retirement_operation_id IS NOT NULL"
			if phase == "batch_pending" {
				table, condition = "digger_batches", "NEW.status = 2"
			}
			require.NoError(t, db.GormDB.Exec(`CREATE FUNCTION injected_stop_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF `+condition+` THEN RAISE EXCEPTION 'injected durable transition failure'; END IF; RETURN NEW; END; $$; CREATE TRIGGER injected_stop_failure BEFORE UPDATE ON `+table+` FOR EACH ROW EXECUTE FUNCTION injected_stop_failure()`).Error)
			_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
			require.Error(t, err)
			var count int64
			require.NoError(t, db.GormDB.Model(&DependencyStop{}).Count(&count).Error)
			require.Zero(t, count)
			require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
			require.NoError(t, db.GormDB.Exec("DROP TRIGGER injected_stop_failure ON "+table).Error)
			_, err = db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
			require.NoError(t, err, "same operation resumes after a rolled-back transition")
		})
	}
}

func TestLegacyStopPostgresVerificationRollsBackWithAggregate(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, _, child, req := legacyStopFixture(t, db, false)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	require.NoError(t, db.GormDB.Exec(`CREATE FUNCTION injected_verification_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status=3 THEN RAISE EXCEPTION 'injected aggregate failure'; END IF; RETURN NEW; END; $$; CREATE TRIGGER injected_verification_failure BEFORE UPDATE ON digger_batches FOR EACH ROW EXECUTE FUNCTION injected_verification_failure()`).Error)
	_, err = db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.Error(t, err)
	saved, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.True(t, saved.DependencyStopPending())
	require.Nil(t, saved.DependencyStop.Verification)
	require.NoError(t, db.GormDB.Exec("DROP TRIGGER injected_verification_failure ON digger_batches").Error)
	_, err = db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.NoError(t, err)
}

func TestLegacyStopPostgresVerificationLocalRaceAndLaterRerun(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, parent, child, req := legacyStopFixture(t, db, false)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	// Change a real callback revision between provider scan and commit.
	require.NoError(t, db.GormDB.Exec("UPDATE digger_jobs SET callback_revision=1 WHERE id=?", parent.ID).Error)
	_, err = db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.ErrorIs(t, err, ErrDependencyStopConflict)
	saved, err := db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.True(t, saved.DependencyStopPending())
	view, err = db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	verified, err := db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.NoError(t, err)
	checkpoint := *verified.VerifiedAt
	parent = dependencySavedJob(t, db, parent.ID)
	parent.Status = scheduler.DiggerJobStarted
	parent.WorkflowRunAttempt = 2
	parent.StatusUpdatedAt = time.Now()
	updated, err := db.AcceptDiggerJobCallback(&parent, org.ID)
	require.NoError(t, err)
	parent = *updated
	result, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Equal(t, scheduler.BatchJobStarted, result.BatchStatus)
	saved, err = db.GetDiggerJob(child.DiggerJobID)
	require.NoError(t, err)
	require.True(t, saved.DependencyStop.VerifiedAt.Equal(checkpoint))
	require.Equal(t, "unknown", saved.DependencyStop.PriorExecution)
	claimed, err := db.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	// Even a later successful prerequisite cannot admit the permanently stopped child.
	parent = dependencySavedJob(t, db, parent.ID)
	parent.Status = scheduler.DiggerJobSucceeded
	parent.StatusUpdatedAt = time.Now().UTC().Add(time.Millisecond)
	updated, err = db.AcceptDiggerJobCallback(&parent, org.ID)
	require.NoError(t, err)
	parent = *updated
	claimed, err = db.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Zero(t, dependencySavedJob(t, db, child.ID).DispatchProtocolVersion)
}

func TestLegacyStopPostgresQueuedLeafAndRunSettlement(t *testing.T) {
	db := commandOriginPostgresDB(t, true)
	batch, org, repo, _, child, req := legacyStopFixture(t, db, false)
	queued, err := db.QueueDiggerJobIfCreated(&child)
	require.NoError(t, err)
	require.True(t, queued)
	plan, err := db.PlanDependencyStop(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	req.Mode, req.ExpectedPlanSHA256 = "execute", plan.PlanSHA256
	stop, err := db.FenceDependencyStop(org.ID, repo.ID, batch.ID, req, "operator")
	require.NoError(t, err)
	batchID := batch.ID.String()
	stage := DiggerRunStage{BatchID: &batchID}
	run := DiggerRun{RepoId: repo.ID, Status: RunApproved, PlanStage: stage, ApplyStage: stage}
	require.NoError(t, db.GormDB.Create(&run).Error)
	project := Project{Name: "infra-k8s", OrganisationID: org.ID, RepoFullName: batch.RepoFullName}
	require.NoError(t, db.GormDB.Create(&project).Error)
	item := DiggerRunQueueItem{ProjectId: project.ID, DiggerRunId: run.ID, DiggerRun: run}
	require.NoError(t, db.GormDB.Create(&item).Error)
	require.ErrorIs(t, db.SettleFailedRunQueueItem(&item, &child), ErrDependencyStopPending)
	var retained int64
	require.NoError(t, db.GormDB.Model(&DiggerRunQueueItem{}).Where("id=?", item.ID).Count(&retained).Error)
	require.Equal(t, int64(1), retained)
	queued, err = db.QueueDiggerJobIfCreated(&child)
	require.NoError(t, err)
	require.False(t, queued, "stale retry cannot undo a fence")
	view, err := db.ReadDependencyStopView(org.ID, repo.ID, batch.ID, req)
	require.NoError(t, err)
	_, err = db.VerifyDependencyStop(org.ID, repo.ID, batch.ID, req, view.StateSHA256, "operator", legacyStopEvidence(*stop))
	require.NoError(t, err)
	require.NoError(t, db.SettleFailedRunQueueItem(&item, &child))
	var saved DiggerRun
	require.NoError(t, db.GormDB.First(&saved, run.ID).Error)
	require.Equal(t, RunFailed, saved.Status)
	require.NoError(t, db.GormDB.Model(&DiggerRunQueueItem{}).Where("id=?", item.ID).Count(&retained).Error)
	require.Zero(t, retained)
}

func TestLegacyStopPostgresMigrationPreservesProducerBase(t *testing.T) {
	db := commandOriginPostgresDB(t, false)
	for _, name := range []string{commandOriginMigration, "20261101120000.sql", "20261112040000_dependency_retirement.sql"} {
		commandOriginApplySQL(t, db.GormDB, filepath.Join("..", "migrations", name))
	}
	before := commandOriginPostgresColumns(t, db.GormDB, "public")
	batch := newCommandOriginBatch(t, db, uuid.New())
	job := commandOriginPostgresJob(t, db, batch, "ordinary")
	commandOriginApplySQL(t, db.GormDB, "../migrations/20261112090000_historical_dependency_stop.sql")
	var retained, added []commandOriginPGColumn
	for _, c := range commandOriginPostgresColumns(t, db.GormDB, "public") {
		if (c.TableName == "digger_dependency_stops" || c.TableName == "digger_dependency_stop_checkpoints") || (c.TableName == "digger_jobs" && c.ColumnName == "dependency_retirement_operation_id") {
			added = append(added, c)
		} else {
			retained = append(retained, c)
		}
	}
	require.Equal(t, before, retained, "no existing columns or defaults change")
	var count int64
	require.NoError(t, db.GormDB.Model(&DependencyStop{}).Count(&count).Error)
	require.Zero(t, count, "migration must not backfill a stop or protocol marker")
	require.Nil(t, dependencySavedJob(t, db, job.ID).DependencyRetirementOperationID)
	// An ordinary previous producer scalar writer still works on its old row.
	var previous dependencyPreviousJob
	require.NoError(t, db.GormDB.First(&previous, job.ID).Error)
	previous.Previous.RunName = "old-producer-control"
	require.NoError(t, db.GormDB.Save(&previous).Error)
	require.NoError(t, db.GormDB.Transaction(func(tx *gorm.DB) error {
		require.NoError(t, tx.Exec("CREATE SCHEMA historical_stop_expected").Error)
		require.NoError(t, tx.Exec("SET LOCAL search_path = historical_stop_expected, public").Error)
		require.NoError(t, tx.Migrator().CreateTable(&DependencyStop{}, &DependencyStopCheckpoint{}, &DiggerJob{}))
		var expected []commandOriginPGColumn
		for _, c := range commandOriginPostgresColumns(t, tx, "historical_stop_expected") {
			if (c.TableName == "digger_dependency_stops" || c.TableName == "digger_dependency_stop_checkpoints") || (c.TableName == "digger_jobs" && c.ColumnName == "dependency_retirement_operation_id") {
				expected = append(expected, c)
			}
		}
		require.Equal(t, expected, added, "migration must match production GORM column types/defaults/nullability")
		return nil
	}))
}
