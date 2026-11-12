package models

import (
	"errors"
	"reflect"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func dependencyStopSnapshot(tx *gorm.DB, graph *dependencyGraph, orgID, repoID uint, request DependencyStopRequest, initial bool) (*DependencyStopSnapshot, error) {
	batch := graph.Batch
	if batch.CommentId == nil || *batch.CommentId <= 0 {
		return nil, stopConflict("original report comment ID is missing")
	}
	var repo Repo
	if err := tx.Where("id = ? AND organisation_id = ? AND repo_full_name = ?", repoID, orgID, batch.RepoFullName).First(&repo).Error; err != nil {
		return nil, err
	}
	if batch.VCS != DiggerVCSGithub || batch.PrNumber != request.PRNumber || batch.CommitSha != request.CommitSHA || batch.Status == scheduler.BatchJobInvalidated || batch.RepoFullName != batch.RepoOwner+"/"+batch.RepoName {
		return nil, ErrDependencyStopConflict
	}
	evidence, err := (&Database{GormDB: tx}).GetCommandProvenance(orgID, repo.RepoFullName, request.PRNumber, request.SourceCommentID)
	if err != nil {
		return nil, err
	}
	if !evidence.Complete {
		return nil, ErrDependencyStopConflict
	}
	var entry *CommandProvenanceBatch
	for i := range evidence.Batches {
		if evidence.Batches[i].BatchID == batch.ID.String() {
			entry = &evidence.Batches[i]
			break
		}
	}
	if entry == nil || !entry.ManifestComplete || entry.Origin.BodySHA256 != request.BodySHA256 || entry.Origin.CommitSHA != request.CommitSHA {
		return nil, ErrDependencyStopConflict
	}
	target := graph.ByID[request.Targets[0].JobID]
	parent := graph.ByID[request.FailedPrerequisite.JobID]
	if target == nil || target.ID != request.Targets[0].DBID || parent == nil || parent.ID != request.FailedPrerequisite.DBID || parent.DependencyRetiredAt != nil || (initial && (parent.Status != scheduler.DiggerJobFailed || parent.WorkflowRunID != request.FailedPrerequisite.WorkflowRunID || parent.WorkflowRunAttempt != request.FailedPrerequisite.WorkflowRunAttempt)) {
		return nil, stopConflict("failed prerequisite no longer matches")
	}
	direct := false
	for _, parentID := range graph.Parents[target.DiggerJobID] {
		if parentID == parent.DiggerJobID {
			direct = true
		}
	}
	if !direct {
		return nil, stopConflict("target has no persisted direct failed prerequisite")
	}
	for _, link := range graph.Links {
		if link.ParentDiggerJobId == target.DiggerJobID {
			return nil, stopConflict("operator stop supports a leaf target only")
		}
	}
	// Lock retained workflow attribution rows before choosing the fence winner.
	var links []GithubDiggerJobLink
	if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("digger_job_id = ?", target.DiggerJobID).Order("id ASC").Find(&links).Error; err != nil {
		return nil, err
	}
	if len(links) == 0 {
		return nil, stopConflict("target workflow lineage row is missing")
	}
	linkStates := make([]DependencyStopLink, 0, len(links))
	for _, link := range links {
		if link.RepoFullName != batch.RepoFullName || link.GithubJobId != 0 || link.GithubWorkflowRunId != 0 {
			return nil, stopConflict("target has workflow attribution")
		}
		linkStates = append(linkStates, DependencyStopLink{ID: link.ID, JobID: link.DiggerJobId, Repo: link.RepoFullName, WorkflowRunID: link.GithubWorkflowRunId, GithubJobID: link.GithubJobId, Deleted: link.DeletedAt.Valid})
	}
	manifest := make([]BatchCommandOriginJob, 0, len(entry.Jobs))
	for _, job := range entry.Jobs {
		manifest = append(manifest, BatchCommandOriginJob{DBID: job.DBID, JobID: job.JobID, Project: job.Project, ProjectCheckID: job.ProjectCheckID})
	}
	edges := make([]DependencyStopEdge, 0, len(graph.Links))
	for _, link := range graph.Links {
		edges = append(edges, DependencyStopEdge{ID: link.ID, JobID: link.DiggerJobId, ParentID: link.ParentDiggerJobId})
	}
	return &DependencyStopSnapshot{BatchID: batch.ID, RepoID: repoID, OrganisationID: orgID, InstallationID: batch.GithubInstallationId, Origin: entry.Origin, AggregateCheckID: entry.AggregateCheckID, ReportCommentID: *batch.CommentId, Manifest: manifest, BatchStatus: batch.Status, Jobs: dependencyStopStates(graph.Jobs), Edges: edges, Links: linkStates}, nil
}

func buildDependencyStopPlan(tx *gorm.DB, graph *dependencyGraph, orgID, repoID uint, request DependencyStopRequest) (*DependencyStopPlan, error) {
	snapshot, err := dependencyStopSnapshot(tx, graph, orgID, repoID, request, true)
	if err != nil {
		return nil, err
	}
	job := graph.ByID[request.Targets[0].JobID]
	if job.DispatchProtocolVersion != 0 || job.DependencyRetiredAt != nil || job.DependencyRetirementOperationID != nil || jobHasExecutionEvidence(*job) || (job.Status != scheduler.DiggerJobCreated && job.Status != scheduler.DiggerJobQueuedForRun) {
		return nil, stopConflict("target is not an unclaimed legacy leaf")
	}
	plan := &DependencyStopPlan{Version: 1, OperationID: request.OperationID, Snapshot: *snapshot, ProposedReport: "future dispatch stopped; prior execution unknown; settlement pending"}
	plan.PlanSHA256, err = dependencyStopPlanHash(request, *snapshot)
	return plan, err
}

func (db *Database) PlanDependencyStop(orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest) (*DependencyStopPlan, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	var plan *DependencyStopPlan
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		graph, err := lockDependencyGraph(tx, batchID)
		if err != nil {
			return err
		}
		plan, err = buildDependencyStopPlan(tx, graph, orgID, repoID, request)
		return err
	})
	return plan, err
}

func (db *Database) GetDependencyStop(orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest) (*DependencyStop, error) {
	var stop DependencyStop
	err := db.GormDB.Where("operation_id = ? AND organisation_id = ? AND repo_id = ? AND batch_id = ?", request.OperationID, orgID, repoID, batchID).First(&stop).Error
	if err != nil {
		return nil, err
	}
	digest, err := dependencyStopHash(request.canonical())
	if err != nil {
		return nil, err
	}
	if !dependencyStopAuditValid(stop) || digest != stop.RequestSHA256 || request.ExpectedPlanSHA256 != stop.PlanSHA256 {
		return nil, ErrDependencyStopConflict
	}
	if err := db.refreshDependencyStopCheckpoint(&stop); err != nil {
		return nil, err
	}
	return &stop, nil
}

func (db *Database) FenceDependencyStop(orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest, actor string) (*DependencyStop, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if actor == "" || len(actor) > 128 {
		return nil, ErrDependencyStopConflict
	}
	var saved DependencyStop
	err := db.WithPublicationBarrier(batchID, func(guarded *Database) error {
		return guarded.GormDB.Transaction(func(tx *gorm.DB) error {
			graph, err := lockDependencyGraph(tx, batchID)
			if err != nil {
				return err
			}
			existing, err := (&Database{GormDB: tx}).GetDependencyStop(orgID, repoID, batchID, request)
			if err == nil {
				saved = *existing
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			plan, err := buildDependencyStopPlan(tx, graph, orgID, repoID, request)
			if err != nil {
				return err
			}
			if plan.PlanSHA256 != request.ExpectedPlanSHA256 {
				return stopConflict("plan changed before execute")
			}
			digest, err := dependencyStopHash(request.canonical())
			if err != nil {
				return err
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			saved = DependencyStop{OperationID: request.OperationID, OrganisationID: orgID, RepoID: repoID, BatchID: batchID, TargetJobDBID: request.Targets[0].DBID, RequestSHA256: digest, PlanSHA256: plan.PlanSHA256, Request: request.canonical(), Snapshot: plan.Snapshot, RequestedBy: actor, FencedAt: now}
			if err := tx.Create(&saved).Error; err != nil {
				return err
			}
			updated := tx.Exec(`UPDATE digger_jobs SET dependency_retirement_operation_id = ?, dependency_retired_at = ?, dependency_failure_job_id = ?, status = 3, status_updated_at = ?, updated_at = ?
            WHERE id = ? AND digger_job_id = ? AND batch_id = ? AND deleted_at IS NULL AND dispatch_protocol_version = 0
            AND dependency_retired_at IS NULL AND dependency_retirement_operation_id IS NULL AND workflow_dispatch_started_at IS NULL
            AND COALESCE(workflow_run_id,0) = 0 AND COALESCE(workflow_run_attempt,0) = 0 AND COALESCE(workflow_run_url,'') IN ('','#') AND status IN (1,6)`,
				request.OperationID, now, request.FailedPrerequisite.JobID, now, now, request.Targets[0].DBID, request.Targets[0].JobID, batchID.String())
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrDependencyStopConflict
			}
			return tx.Model(&DiggerBatch{}).Where("id = ?", batchID).Update("status", scheduler.BatchJobStarted).Error
		})
	})
	return &saved, err
}

type DependencyStopView struct {
	Operation   DependencyStop
	StateSHA256 string
	Snapshot    DependencyStopSnapshot
}

func readDependencyStopView(tx *gorm.DB, orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest) (*DependencyStopView, error) {
	graph, err := lockDependencyGraph(tx, batchID)
	if err != nil {
		return nil, err
	}
	var stop DependencyStop
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("operation_id = ? AND organisation_id = ? AND repo_id = ? AND batch_id = ?", request.OperationID, orgID, repoID, batchID).First(&stop).Error; err != nil {
		return nil, err
	}
	digest, err := dependencyStopHash(request.canonical())
	if err != nil {
		return nil, err
	}
	if digest != stop.RequestSHA256 || request.ExpectedPlanSHA256 != stop.PlanSHA256 {
		return nil, ErrDependencyStopConflict
	}
	snapshot, err := dependencyStopSnapshot(tx, graph, orgID, repoID, request, false)
	if err != nil {
		return nil, err
	}
	target := graph.ByID[request.Targets[0].JobID]
	if target == nil || !dependencyStopMatchesJob(stop, *target) || snapshot.ReportCommentID != stop.Snapshot.ReportCommentID || snapshot.AggregateCheckID != stop.Snapshot.AggregateCheckID || !reflect.DeepEqual(snapshot.Origin, stop.Snapshot.Origin) || !reflect.DeepEqual(snapshot.Manifest, stop.Snapshot.Manifest) || !reflect.DeepEqual(snapshot.Edges, stop.Snapshot.Edges) || !reflect.DeepEqual(snapshot.Links, stop.Snapshot.Links) {
		return nil, ErrDependencyStopConflict
	}
	snapshot.BatchStatus = 0
	state, err := dependencyStopHash(snapshot)
	if err != nil {
		return nil, err
	}
	if err := loadCurrentDependencyCheckpoint(tx, &stop, graph.Jobs); err != nil {
		return nil, err
	}
	return &DependencyStopView{Operation: stop, StateSHA256: state, Snapshot: *snapshot}, nil
}

func (db *Database) ReadDependencyStopView(orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest) (*DependencyStopView, error) {
	var view *DependencyStopView
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		var err error
		view, err = readDependencyStopView(tx, orgID, repoID, batchID, request)
		return err
	})
	return view, err
}

func (db *Database) VerifyDependencyStop(orgID, repoID uint, batchID uuid.UUID, request DependencyStopRequest, stateSHA256, actor string, evidence DependencyStopVerification) (*DependencyStop, error) {
	if actor == "" || len(actor) > 128 {
		return nil, ErrDependencyStopConflict
	}
	var stop DependencyStop
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		view, err := readDependencyStopView(tx, orgID, repoID, batchID, request)
		if err != nil {
			return err
		}
		stop = view.Operation
		if stateSHA256 != view.StateSHA256 || !evidence.ValidFor(stop) || !checkpointParentMatches(evidence, stop, view.Snapshot.Jobs) || time.Since(evidence.FinishedAt) < 0 || time.Since(evidence.FinishedAt) > time.Minute {
			return ErrDependencyStopConflict
		}
		input, err := dependencyCheckpointInput(view.StateSHA256, evidence)
		if err != nil {
			return err
		}
		var cp DependencyStopCheckpoint
		lookup := tx.Where("operation_id=? AND input_sha256=?", stop.OperationID, input).First(&cp).Error
		if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		if errors.Is(lookup, gorm.ErrRecordNotFound) {
			local, err := dependencyStopHash(view.Snapshot.Jobs)
			if err != nil {
				return err
			}
			now := time.Now().UTC().Truncate(time.Microsecond).Add(time.Microsecond)
			cp = DependencyStopCheckpoint{ID: uuid.New(), OperationID: stop.OperationID, InputSHA256: input, LocalStateSHA256: local, JobStates: view.Snapshot.Jobs, Verification: evidence, VerifiedAt: now, VerifiedBy: actor}
			if err := tx.Create(&cp).Error; err != nil {
				return err
			}
		}
		if stop.VerifiedAt == nil {
			stop.VerifiedAt, stop.VerifiedBy, stop.Verification = &cp.VerifiedAt, cp.VerifiedBy, &cp.Verification
			if err := tx.Model(&stop).Where("operation_id=? AND verified_at IS NULL", stop.OperationID).Select("verified_at", "verified_by", "verification").Updates(&stop).Error; err != nil {
				return err
			}
		}
		stop.CurrentCheckpoint = &cp
		_, err = (&Database{GormDB: tx}).ReconcileFailedDependencies(batchID)
		return err
	})
	return &stop, err
}
