package models

import (
	"errors"
	"github.com/diggerhq/digger/libs/scheduler"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SettleFailedRunQueueItem fails the owning run and removes its queue item in
// one transaction. A stale queue snapshot cannot terminate a different stage.
func (db *Database) SettleFailedRunQueueItem(item *DiggerRunQueueItem, job *DiggerJob) error {
	return db.GormDB.Transaction(func(tx *gorm.DB) error {
		var batch DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&batch, "id = ?", job.BatchID).Error; err != nil {
			return err
		}
		var current DiggerJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id = ? AND digger_job_id = ? AND batch_id = ?", job.ID, job.DiggerJobID, batch.ID.String()).Error; err != nil {
			return err
		}
		if current.Status != scheduler.DiggerJobFailed {
			return errors.New("job is not terminally failed")
		}
		jobs := []DiggerJob{current}
		if err := loadDependencyStops(tx, jobs); err != nil {
			return err
		}
		if jobs[0].DependencyStopPending() {
			return ErrDependencyStopPending
		}
		var run DiggerRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&run, item.DiggerRunId).Error; err != nil {
			return err
		}
		if run.Status != RunFailed {
			if run.Status != item.DiggerRun.Status || (run.Status != RunQueued && run.Status != RunApproved) {
				return ErrDiggerJobTransitionConflict
			}
			stageID := run.PlanStageId
			if run.Status == RunApproved {
				stageID = run.ApplyStageId
			}
			var stage DiggerRunStage
			if err := tx.First(&stage, "id = ? AND batch_id = ?", stageID, current.BatchID).Error; err != nil {
				return err
			}
			updated := tx.Model(&DiggerRun{}).Where("id = ? AND status = ?", run.ID, run.Status).Update("status", RunFailed)
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrDiggerJobTransitionConflict
			}
		}
		return tx.Where("id = ? AND digger_run_id = ?", item.ID, run.ID).Delete(&DiggerRunQueueItem{}).Error
	})
}
