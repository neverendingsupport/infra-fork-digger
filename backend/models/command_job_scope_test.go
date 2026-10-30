package models

import (
	"encoding/json"
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCommandManifestRejectsDifferentExecutionScope(t *testing.T) {
	for _, stage := range []string{"before_seal", "after_seal"} {
		for _, field := range []string{"command", "repository", "pr", "missing_pr"} {
			t.Run(stage+"/"+field, func(t *testing.T) {
				db := newCommandOriginDB(t)
				org := setCommandOriginScope(t, db, 77)
				batch := newCommandOriginBatch(t, db, uuid.New())
				require.NoError(t, db.CreateBatchCommandOrigin(batch.ID, commandSource(), []string{"a"}))
				job := newCommandOriginJob(t, db, batch, "a")
				if stage == "after_seal" {
					require.NoError(t, db.SealBatchCommandOrigin(batch.ID))
				}
				var spec scheduler.JobJson
				require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
				switch field {
				case "command":
					spec.JobType = string(scheduler.DiggerCommandApply)
				case "repository":
					spec.Namespace = "other/repo"
				case "pr":
					different := batch.PrNumber + 1
					spec.PullRequestNumber = &different
				case "missing_pr":
					spec.PullRequestNumber = nil
				}
				encoded, err := json.Marshal(spec)
				require.NoError(t, err)
				require.NoError(t, db.GormDB.Model(&job).Update("serialized_job_spec", encoded).Error)
				if stage == "before_seal" {
					require.Error(t, db.SealBatchCommandOrigin(batch.ID))
				} else {
					got, err := db.GetCommandProvenance(org, batch.RepoFullName, batch.PrNumber, commandSource().CommentID)
					require.NoError(t, err)
					require.Len(t, got.Batches, 1)
					require.False(t, got.Batches[0].ManifestComplete)
					require.Zero(t, got.Batches[0].Jobs[0].Status)
				}
			})
		}
	}
}
