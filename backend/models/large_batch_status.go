package models

import (
	"errors"

	"github.com/diggerhq/digger/libs/scheduler"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// updateLargeBatchStatus derives ordinary progress without qualifying retirement.
// The provenance/retirement limit must not prevent a runner acknowledging start.
func (db *Database) updateLargeBatchStatus(batch *DiggerBatch) error {
	return db.GormDB.Transaction(func(tx *gorm.DB) error {
		var current DiggerBatch
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id = ?", batch.ID).Error; err != nil {
			return err
		}
		var counts struct{ Total, Pending, Failed, Deleted int64 }
		if err := tx.Unscoped().Model(&DiggerJob{}).Where("batch_id = ?", batch.ID.String()).Select(`COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN status IS NULL OR status NOT IN (3, 5) THEN 1 ELSE 0 END), 0) AS pending,
            COALESCE(SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END), 0) AS failed,
            COALESCE(SUM(CASE WHEN deleted_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS deleted`).Scan(&counts).Error; err != nil {
			return err
		}
		if counts.Deleted != 0 {
			return errors.New("batch aggregate contains deleted job identities")
		}
		next := current.Status
		if current.Status != scheduler.BatchJobInvalidated {
			if counts.Total > 0 && counts.Pending == 0 {
				next = scheduler.BatchJobSucceeded
				if counts.Failed > 0 {
					next = scheduler.BatchJobFailed
				}
			} else if current.Status == scheduler.BatchJobFailed || current.Status == scheduler.BatchJobSucceeded {
				next = scheduler.BatchJobStarted
			}
		}
		if next != current.Status {
			if err := tx.Model(&DiggerBatch{}).Where("id = ?", current.ID).Update("status", next).Error; err != nil {
				return err
			}
		}
		batch.Status = next
		return nil
	})
}
