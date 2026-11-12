package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeLogValueKeepsOneBoundedRecord(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewJSONHandler(&output, nil)
			if format == "text" {
				handler = slog.NewTextHandler(&output, nil)
			}
			value := SafeLogValue("actor\r\nforged=record\t\x00" + strings.Repeat("x", 1000))
			require.NotContains(t, value, "\n")
			require.NotContains(t, value, "\r")
			require.LessOrEqual(t, len(value), 6*256+2)
			slog.New(handler).Info("digger.test.received", "actor", value)
			require.Equal(t, 1, strings.Count(output.String(), "\n"))
			if format == "json" {
				var decoded map[string]interface{}
				require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
				require.Equal(t, value, decoded["actor"])
			}
		})
	}
}
