package logging

import (
	"strconv"
	"strings"
)

// SafeLogValue bounds request identifiers and escapes control characters even
// when a caller uses a plain-text handler instead of the production JSON handler.
func SafeLogValue(value string) string {
	if len(value) > 256 {
		value = value[:256]
	}
	value = strings.ReplaceAll(value, "\r", `\r`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strconv.QuoteToASCII(value)
}
