package controllers

import "github.com/diggerhq/digger/libs/scheduler"

// skipForDraftState reports whether a pull_request event should do nothing
// because of the PR's current draft state, which may differ from the event's.
func skipForDraftState(allowDraftPRs bool, isDraft bool, action string, command scheduler.DiggerCommand) bool {
	if allowDraftPRs {
		return false
	}
	// A late event mustn't unlock a PR that's ready again.
	if action == "converted_to_draft" && !isDraft {
		return true
	}
	// A draft still releases its locks, e.g. when closed.
	return isDraft && command != scheduler.DiggerCommandUnlock
}
