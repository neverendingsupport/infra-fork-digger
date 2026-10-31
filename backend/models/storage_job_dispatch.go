package models

import (
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
)

// ClaimDiggerJobDispatch commits the possible dispatch before calling the CI API.
// A failed or lost provider response retains this state: it does not permit resend.
func (db *Database) ClaimDiggerJobDispatch(job *DiggerJob) (bool, error) {
	// Raw SQL deliberately writes the otherwise read-only model field. The
	// marker remains consumed even if a later callback reports status=created.
	now := time.Now().UTC()
	result := db.GormDB.Exec(`UPDATE digger_jobs
		SET workflow_dispatch_started_at = ?, status = ?, status_updated_at = ?, updated_at = ?
		WHERE id = ? AND digger_job_id = ? AND batch_id = ? AND deleted_at IS NULL
		AND workflow_dispatch_started_at IS NULL AND status IN (?, ?)`,
		now, scheduler.DiggerJobTriggered, now, now,
		job.ID, job.DiggerJobID, job.BatchID,
		scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun)
	return result.RowsAffected == 1, result.Error
}

// QueueDiggerJobIfCreated cannot move a concurrent dispatch back into the queue.
func (db *Database) QueueDiggerJobIfCreated(job *DiggerJob) (bool, error) {
	result := db.GormDB.Model(&DiggerJob{}).
		Where("id = ? AND digger_job_id = ? AND batch_id = ? AND status = ? AND workflow_dispatch_started_at IS NULL",
			job.ID, job.DiggerJobID, job.BatchID, scheduler.DiggerJobCreated).
		Updates(map[string]interface{}{"status": scheduler.DiggerJobQueuedForRun, "status_updated_at": time.Now().UTC()})
	return result.RowsAffected == 1, result.Error
}

// CountActiveDiggerJobDispatches also counts claimed jobs whose first callback
// reports created. That report does not make the workflow stop using capacity.
func (db *Database) CountActiveDiggerJobDispatches(batchID string) (int64, error) {
	var count int64
	err := db.GormDB.Model(&DiggerJob{}).Where("batch_id = ?", batchID).
		Where(`status IN ? OR (workflow_dispatch_started_at IS NOT NULL AND status IN ?)`,
			[]scheduler.DiggerJobStatus{scheduler.DiggerJobTriggered, scheduler.DiggerJobStarted},
			[]scheduler.DiggerJobStatus{scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun}).
		Count(&count).Error
	return count, err
}
