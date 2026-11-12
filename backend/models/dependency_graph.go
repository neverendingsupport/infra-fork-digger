package models

import (
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type dependencyGraph struct {
	Batch   DiggerBatch
	Jobs    []DiggerJob
	ByID    map[string]*DiggerJob
	Parents map[string][]string
	Links   []DiggerJobParentLink
}

// lockDependencyGraph keeps operator stops and ordinary reconciliation on the
// same bounded graph validation and batch/job lock order.
func lockDependencyGraph(tx *gorm.DB, batchID uuid.UUID) (*dependencyGraph, error) {
	var batch DiggerBatch
	var jobs []DiggerJob
	var links []DiggerJobParentLink
	var byID map[string]*DiggerJob
	var parents map[string][]string
	err := func() error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", batchID).Error; err != nil {
			return err
		}
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Preload("DiggerJobSummary").Where("batch_id = ?", batchID.String()).Order("id ASC").Limit(MaxCommandProvenanceJobs + 1).Find(&jobs).Error; err != nil {
			return err
		}
		if len(jobs) > MaxCommandProvenanceJobs {
			return ErrProvenanceTooLarge
		}
		byID = map[string]*DiggerJob{}
		ids := make([]string, 0, len(jobs))
		for i := range jobs {
			job := &jobs[i]
			if job.DeletedAt.Valid || job.DiggerJobID == "" || byID[job.DiggerJobID] != nil {
				return errors.New("dependency graph has missing or duplicate job identities")
			}
			byID[job.DiggerJobID] = job
			ids = append(ids, job.DiggerJobID)
		}
		if len(ids) > 0 {
			if err := tx.Where("digger_job_id IN ? OR parent_digger_job_id IN ?", ids, ids).Order("id ASC").Find(&links).Error; err != nil {
				return err
			}
		}
		parents = map[string][]string{}
		for _, link := range links {
			if byID[link.DiggerJobId] == nil || byID[link.ParentDiggerJobId] == nil {
				return errors.New("dependency graph crosses batch or has missing job")
			}
			parents[link.DiggerJobId] = append(parents[link.DiggerJobId], link.ParentDiggerJobId)
		}
		// Validate the whole graph before persisting any fence.
		visited, visiting := map[string]bool{}, map[string]bool{}
		var visit func(string) error
		visit = func(id string) error {
			if visiting[id] {
				return errors.New("dependency graph contains a cycle")
			}
			if visited[id] {
				return nil
			}
			visiting[id] = true
			for _, parent := range parents[id] {
				if err := visit(parent); err != nil {
					return err
				}
			}
			visiting[id], visited[id] = false, true
			return nil
		}
		for _, id := range ids {
			if err := visit(id); err != nil {
				return err
			}
		}
		return loadDependencyStops(tx, jobs)
	}()
	if err != nil {
		return nil, err
	}
	return &dependencyGraph{Batch: batch, Jobs: jobs, ByID: byID, Parents: parents, Links: links}, nil
}
