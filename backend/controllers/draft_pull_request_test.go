package controllers

import (
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/assert"
)

func TestSkipForDraftState(t *testing.T) {
	plan, unlock := scheduler.DiggerCommandPlan, scheduler.DiggerCommandUnlock
	for _, tc := range []struct {
		name          string
		allowDraftPRs bool
		isDraft       bool
		action        string
		command       scheduler.DiggerCommand
		skip          bool
	}{
		{"push to a draft", false, true, "synchronize", plan, true},
		{"push whose payload said draft, PR now ready", false, false, "synchronize", plan, false},
		{"ready_for_review for a PR back in draft", false, true, "ready_for_review", plan, true},
		{"draft reopened", false, true, "reopened", scheduler.DiggerCommandLock, true},
		{"draft closed", false, true, "closed", unlock, false},
		{"converted to draft", false, true, "converted_to_draft", unlock, false},
		{"late converted_to_draft after ready", false, false, "converted_to_draft", unlock, true},
		{"converted to draft with a custom plan", false, true, "converted_to_draft", plan, true},
		{"merged", false, false, "closed", unlock, false},
		{"drafts allowed", true, true, "synchronize", plan, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.skip, skipForDraftState(tc.allowDraftPRs, tc.isDraft, tc.action, tc.command))
		})
	}
}
