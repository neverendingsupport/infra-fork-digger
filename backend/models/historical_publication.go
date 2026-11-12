package models

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// HistoricalPublication is one authorized content/destination snapshot. Writers
// must use this object, never replace its IDs with a later mutable batch read.
type HistoricalPublication struct {
	Batch  DiggerBatch
	Jobs   []DiggerJob
	Checks map[string]bool
}

// MatchesContent rejects options prepared before a callback, checkpoint or
// resource summary changed. The guard is local and never enters the API DTO.
func (p *HistoricalPublication) MatchesContent(batch *DiggerBatch) bool {
	return batch.historicalPublicationSHA256 != "" && batch.historicalPublicationSHA256 == p.Batch.historicalPublicationSHA256
}

func (db *Database) HistoricalPublication(id uuid.UUID) (*HistoricalPublication, error) {
	var count int64
	if err := db.GormDB.Unscoped().Model(&DiggerJob{}).Where("batch_id=? AND dependency_retirement_operation_id IS NOT NULL", id.String()).Count(&count).Error; err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	var publication *HistoricalPublication
	err := db.GormDB.Transaction(func(tx *gorm.DB) error {
		graph, err := lockDependencyGraph(tx, id)
		if err != nil {
			return err
		}
		checks := map[string]bool{}
		for _, job := range graph.Jobs {
			if job.DependencyRetirementOperationID == nil {
				continue
			}
			stop := job.DependencyStop
			if stop == nil || graph.Batch.CommentId == nil || *graph.Batch.CommentId != stop.Snapshot.ReportCommentID || graph.Batch.CheckRunId == nil || *graph.Batch.CheckRunId != stop.Snapshot.AggregateCheckID || graph.Batch.RepoFullName != stop.Snapshot.Origin.RepoFullName || graph.Batch.RepoOwner+"/"+graph.Batch.RepoName != stop.Snapshot.Origin.RepoFullName || graph.Batch.GithubInstallationId != stop.Snapshot.InstallationID {
				return ErrDependencyStopConflict
			}
			evidence, err := (&Database{GormDB: tx}).GetCommandProvenance(stop.OrganisationID, stop.Snapshot.Origin.RepoFullName, stop.Request.PRNumber, stop.Request.SourceCommentID)
			if err != nil {
				return err
			}
			bound := false
			for _, entry := range evidence.Batches {
				if entry.BatchID == id.String() && entry.ManifestComplete && entry.Origin == stop.Snapshot.Origin && entry.AggregateCheckID == stop.Snapshot.AggregateCheckID {
					bound = true
				}
			}
			if !evidence.Complete || !bound {
				return ErrDependencyStopConflict
			}
			checks[stop.Snapshot.AggregateCheckID] = true
			for _, entry := range stop.Snapshot.Manifest {
				checks[entry.ProjectCheckID] = true
			}
		}
		effectiveHistoricalStatus(&graph.Batch, graph.Jobs)
		// Hash before attaching Batch pointers to jobs, avoiding recursive content.
		content, err := json.Marshal(struct {
			Batch DiggerBatch
			Jobs  []DiggerJob
		}{graph.Batch, graph.Jobs})
		if err != nil {
			return err
		}
		graph.Batch.historicalPublicationSHA256 = fmt.Sprintf("%x", sha256.Sum256(content))
		for i := range graph.Jobs {
			graph.Jobs[i].Batch = &graph.Batch
		}
		publication = &HistoricalPublication{Batch: graph.Batch, Jobs: graph.Jobs, Checks: checks}
		return nil
	})
	return publication, err
}
