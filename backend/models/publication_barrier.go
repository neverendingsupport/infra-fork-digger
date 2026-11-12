package models

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"hash/fnv"
	"sync"
)

var publicationFixtureLocks sync.Map

// WithPublicationBarrier serializes the actual provider write with first fence
// admission. PostgreSQL owns the cross-replica lock for the whole transaction.
// SQLite is only the local controller fixture; it has no advisory-lock support.
func (db *Database) WithPublicationBarrier(id uuid.UUID, fn func(*Database) error) error {
	if db.GormDB.Dialector.Name() != "postgres" {
		value, _ := publicationFixtureLocks.LoadOrStore(id, &sync.Mutex{})
		lock := value.(*sync.Mutex)
		lock.Lock()
		defer lock.Unlock()
		return fn(db)
	}
	return db.GormDB.Transaction(func(tx *gorm.DB) error {
		hash := fnv.New32a()
		_, _ = hash.Write(id[:])
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?, ?)", int32(0x44494750), int32(hash.Sum32())).Error; err != nil {
			return err
		}
		return fn(&Database{GormDB: tx})
	})
}
