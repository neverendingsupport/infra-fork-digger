package controllers

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDependencyRecoveryStructuredLogEscapesActor(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	dependencyRecoveryRegisteredAPI(t, "safe_logging")
	rendered := output.String()
	decoder := json.NewDecoder(strings.NewReader(rendered))
	found := false
	for {
		var record map[string]interface{}
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "each log must remain a complete JSON record")
		if record["msg"] == "digger.dependencies.recovery_received" {
			found = true
			actor, ok := record["actor"].(string)
			require.True(t, ok)
			require.NotContains(t, actor, "\n")
			require.NotContains(t, actor, "\r")
			require.Contains(t, actor, "operator")
			require.LessOrEqual(t, len(actor), 6*256+2)
		}
	}
	require.True(t, found)
	require.NotContains(t, rendered, "\nforged=record")
	require.NotEmpty(t, strings.TrimSpace(rendered))
}
