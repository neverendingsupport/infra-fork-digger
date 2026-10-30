package models

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MaxCommandProvenanceBatches = 20
	MaxCommandProvenanceJobs    = 500
)

var ErrProvenanceTooLarge = errors.New("command provenance exceeds supported bounds")

// GitHubCommandSource is supplied only by authenticated issue_comment/created
// ingress. BodySHA256 hashes the exact accepted body; report comment IDs are not sources.
type GitHubCommandSource struct {
	RepositoryID int64
	CommentID    int64
	BodySHA256   string
}

// BatchCommandOrigin is additive so whole-batch saves cannot overwrite provenance.
// Source fields are create-only. Only SealBatchCommandOrigin writes the manifest,
// under a row lock and a conditional update; there is no reseal or upsert path.
type BatchCommandOrigin struct {
	BatchID              uuid.UUID               `gorm:"type:uuid;primaryKey;<-:create"`
	RepositoryID         int64                   `gorm:"not null;<-:create"`
	RepoFullName         string                  `gorm:"not null;index:idx_command_origin_source,priority:1;<-:create"`
	PrNumber             int                     `gorm:"not null;index:idx_command_origin_source,priority:2;<-:create"`
	SourceCommentID      int64                   `gorm:"not null;index:idx_command_origin_source,priority:3;<-:create"`
	BodySHA256           string                  `gorm:"size:64;not null;<-:create"`
	CommitSHA            string                  `gorm:"not null;<-:create"`
	Command              scheduler.DiggerCommand `gorm:"not null;<-:create"`
	GithubInstallationID int64                   `gorm:"not null;<-:create"`
	AggregateCheckID     string                  `gorm:"not null;<-:create"`
	ExpectedProjects     []string                `gorm:"serializer:json;type:text;not null;<-:create"`
	JobManifest          []BatchCommandOriginJob `gorm:"serializer:json;type:text"`
	Sealed               bool                    `gorm:"not null;default:false"`
}

// BatchCommandOriginJob binds both database and external job identities.
type BatchCommandOriginJob struct {
	DBID           uint   `json:"db_id"`
	JobID          string `json:"job_id"`
	Project        string `json:"project"`
	ProjectCheckID string `json:"project_check_id"`
}

func (db *Database) CreateBatchCommandOrigin(batchID uuid.UUID, source GitHubCommandSource, expectedProjects []string) error {
	if batchID == uuid.Nil || source.RepositoryID <= 0 || source.CommentID <= 0 {
		return errors.New("command origin requires positive source IDs and a batch UUID")
	}
	digest, err := hex.DecodeString(source.BodySHA256)
	if err != nil || len(digest) != 32 {
		return errors.New("command origin requires a SHA256 body digest")
	}
	projects, err := commandOriginProjects(expectedProjects)
	if err != nil {
		return err
	}
	err = db.GormDB.Transaction(func(tx *gorm.DB) error {
		var batch DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", batchID).Error; err != nil {
			return err
		}
		if batch.VCS != DiggerVCSGithub || (batch.BatchType != scheduler.DiggerCommandPlan && batch.BatchType != scheduler.DiggerCommandApply) {
			return errors.New("command origin requires a GitHub plan or apply batch")
		}
		if batch.PrNumber <= 0 || strings.TrimSpace(batch.RepoFullName) == "" || strings.TrimSpace(batch.CommitSha) == "" || batch.GithubInstallationId <= 0 || strings.TrimSpace(commandOriginString(batch.CheckRunId)) == "" {
			return errors.New("command origin requires repository, PR, head, installation and aggregate check identities")
		}
		var existingJobs []uint
		if err := tx.Unscoped().Model(&DiggerJob{}).Where("batch_id = ?", batchID.String()).Limit(1).Pluck("id", &existingJobs).Error; err != nil {
			return err
		}
		if len(existingJobs) != 0 {
			return errors.New("command origin must be captured before job creation")
		}
		// Never use batch.CommentId: it can identify a mutable report comment.
		origin := BatchCommandOrigin{
			BatchID: batchID, RepositoryID: source.RepositoryID,
			RepoFullName: batch.RepoFullName, PrNumber: batch.PrNumber,
			SourceCommentID: source.CommentID, BodySHA256: source.BodySHA256,
			CommitSHA: batch.CommitSha, Command: batch.BatchType,
			GithubInstallationID: batch.GithubInstallationId,
			AggregateCheckID:     commandOriginString(batch.CheckRunId), ExpectedProjects: projects,
		}
		return tx.Create(&origin).Error
	})
	if err == nil {
		slog.Info("digger.command_origin.created", "batch_id", batchID)
	}
	return err
}

func commandOriginProjects(projects []string) ([]string, error) {
	if len(projects) > MaxCommandProvenanceJobs {
		return nil, ErrProvenanceTooLarge
	}
	if len(projects) == 0 {
		return nil, errors.New("command origin requires expected projects")
	}
	result := slices.Clone(projects)
	for _, project := range result {
		if strings.TrimSpace(project) == "" {
			return nil, errors.New("command origin project cannot be empty")
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func (db *Database) SealBatchCommandOrigin(batchID uuid.UUID) error {
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		// Match the create path's batch-before-origin lock order. A duplicate
		// producer must not deadlock with sealing the original manifest.
		var batch DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", batchID).Error; err != nil {
			return err
		}
		var origin BatchCommandOrigin
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&origin, "batch_id = ?", batchID).Error; err != nil {
			return err
		}
		if origin.Sealed {
			return errors.New("command origin is already sealed")
		}
		if !commandOriginMatchesBatch(origin, batch) {
			return errors.New("batch identity differs from command origin")
		}
		jobs, err := commandOriginJobs(tx, batchID)
		if err != nil {
			return err
		}
		manifest, err := commandOriginManifest(origin, jobs)
		if err != nil {
			return err
		}
		// Use the model serializer, retaining an atomic false -> true fence even on
		// databases that do not implement SELECT FOR UPDATE (SQLite tests).
		result := tx.Model(&BatchCommandOrigin{}).Where("batch_id = ? AND sealed = ?", batchID, false).
			Select("JobManifest", "Sealed").Updates(&BatchCommandOrigin{JobManifest: manifest, Sealed: true})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("command origin seal lost its write fence")
		}
		return nil
	})
	if err == nil {
		slog.Info("digger.command_origin.sealed", "batch_id", batchID)
	} else {
		slog.Warn("digger.command_origin.seal_rejected", "batch_id", batchID)
	}
	return err
}

func commandOriginMatchesBatch(origin BatchCommandOrigin, batch DiggerBatch) bool {
	return !batch.DeletedAt.Valid && batch.ID == origin.BatchID && batch.VCS == DiggerVCSGithub &&
		batch.RepoFullName == origin.RepoFullName && batch.PrNumber == origin.PrNumber &&
		batch.CommitSha == origin.CommitSHA && batch.BatchType == origin.Command &&
		batch.GithubInstallationId == origin.GithubInstallationID &&
		commandOriginString(batch.CheckRunId) == origin.AggregateCheckID
}

func commandOriginJobs(tx *gorm.DB, batchID uuid.UUID) ([]DiggerJob, error) {
	var jobs []DiggerJob
	// Include deleted rows so a deleted job cannot disappear from completeness.
	// Load serialized jobs only to check identities; none of their contents are
	// exposed. Terraform outputs and reporter/transport specs are not read.
	err := tx.Unscoped().Select("id", "deleted_at", "batch_id", "digger_job_id", "project_name", "check_run_id", "serialized_job_spec", "status", "workflow_run_url", "workflow_run_id", "workflow_run_attempt").
		Where("batch_id = ?", batchID.String()).Order("id ASC").Limit(MaxCommandProvenanceJobs + 1).Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if len(jobs) > MaxCommandProvenanceJobs {
		return nil, ErrProvenanceTooLarge
	}
	return jobs, nil
}

func commandOriginManifest(origin BatchCommandOrigin, jobs []DiggerJob) ([]BatchCommandOriginJob, error) {
	projects, err := commandOriginProjects(origin.ExpectedProjects)
	if err != nil {
		return nil, err
	}
	if len(jobs) != len(projects) {
		return nil, errors.New("job count differs from expected projects")
	}
	seenJobs, seenChecks := make(map[string]bool), make(map[string]bool)
	manifest := make([]BatchCommandOriginJob, 0, len(jobs))
	for _, job := range jobs {
		var spec scheduler.JobJson
		if err := json.Unmarshal(job.SerializedJobSpec, &spec); err != nil {
			// Do not wrap decoder errors that might expose serialized credentials.
			return nil, errors.New("job has an invalid serialized identity")
		}
		check := commandOriginString(job.CheckRunId)
		if job.DeletedAt.Valid || job.ID == 0 || strings.TrimSpace(job.DiggerJobID) == "" || strings.TrimSpace(check) == "" || job.BatchID == nil || *job.BatchID != origin.BatchID.String() {
			return nil, errors.New("job has missing or deleted identity")
		}
		if spec.Commit != origin.CommitSHA || spec.ProjectName != job.ProjectName || !slices.Contains(projects, job.ProjectName) ||
			spec.JobType != string(origin.Command) || spec.Namespace != origin.RepoFullName ||
			spec.PullRequestNumber == nil || *spec.PullRequestNumber != origin.PrNumber {
			return nil, errors.New("job execution scope differs from command origin")
		}
		if seenJobs[job.DiggerJobID] || seenChecks[check] || check == origin.AggregateCheckID {
			return nil, errors.New("job or check identity is duplicated")
		}
		seenJobs[job.DiggerJobID], seenChecks[check] = true, true
		manifest = append(manifest, BatchCommandOriginJob{DBID: job.ID, JobID: job.DiggerJobID, Project: job.ProjectName, ProjectCheckID: check})
	}
	slices.SortFunc(manifest, func(a, b BatchCommandOriginJob) int { return strings.Compare(a.Project, b.Project) })
	for i, project := range projects {
		if manifest[i].Project != project {
			return nil, errors.New("job manifest differs from expected projects")
		}
	}
	return manifest, nil
}

func commandOriginString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
