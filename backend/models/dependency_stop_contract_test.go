package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyStopFrozenOutcomeContract(t *testing.T) {
	schema, err := os.ReadFile("testdata/dependency-stop/outcome-v1.schema.json")
	require.NoError(t, err)
	digest := sha256.Sum256(schema)
	require.Equal(t, "da2654c08d9cdf0db0921edf6ad2e6b015996f3f4278a9530e7f3295b2d83ce7", hex.EncodeToString(digest[:]), "coordinate schema changes with the separately reviewed consumer")
	for _, phase := range []string{"pending", "verified"} {
		t.Run(phase, func(t *testing.T) {
			data, err := os.ReadFile("testdata/dependency-stop/" + phase + "-v1.json")
			require.NoError(t, err)
			var outcome ExecutionOutcome
			require.NoError(t, json.Unmarshal(data, &outcome))
			encoded, err := json.Marshal(outcome)
			require.NoError(t, err)
			require.JSONEq(t, string(data), string(encoded), "the production DTO must preserve the complete frozen contract")
			require.Equal(t, "unknown", outcome.PriorExecution)
			require.Equal(t, "future_dispatch_fenced", outcome.Kind)
			require.Equal(t, outcome.RecoveryID, outcome.FenceID)
			require.Equal(t, int64(6746869800), outcome.Binding.ReportCommentID)
			if phase == "pending" {
				require.Nil(t, outcome.VerifiedAt)
				require.Nil(t, outcome.Settlement)
			} else {
				require.NotNil(t, outcome.VerifiedAt)
				require.NotNil(t, outcome.Settlement)
			}
		})
	}
	// Native outcome JSON remains compatible with the signed producer. New
	// historical fields must never appear as null/zero additions to that shape.
	data := `{"version":1,"kind":"never_dispatched","reason":"failed_dependency","prerequisite_job_id":"parent","retired_at":"2026-11-12T04:00:00Z","dispatch_fenced":true}`
	var native ExecutionOutcome
	require.NoError(t, json.Unmarshal([]byte(data), &native))
	encoded, err := json.Marshal(native)
	require.NoError(t, err)
	require.JSONEq(t, data, string(encoded))
}
