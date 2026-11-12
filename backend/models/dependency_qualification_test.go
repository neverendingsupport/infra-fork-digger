package models

import (
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDependencyQualificationProducerGate(t *testing.T) {
	for _, tc := range []struct {
		flag string
		vcs  DiggerVCSType
		want int
	}{
		{"", DiggerVCSGithub, 0}, {"false", DiggerVCSGithub, 0}, {"invalid", DiggerVCSGithub, 0},
		{"true", DiggerVCSGithub, 1}, {"true", DiggerVCSGitlab, 0}, {"true", DiggerVCSBitbucket, 0},
	} {
		t.Run(string(tc.vcs)+"/"+tc.flag, func(t *testing.T) {
			t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", tc.flag)
			db := newCommandOriginDB(t)
			batch := newCommandOriginBatch(t, db, uuid.New())
			batch.VCS = tc.vcs
			require.NoError(t, db.GormDB.Save(&batch).Error)
			job, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "child")
			require.NoError(t, err)
			require.Equal(t, tc.want, dependencySavedJob(t, db, job.ID).DispatchProtocolVersion)
		})
	}
}

func TestDependencyQualificationUnqualifiedTerminalCompatibility(t *testing.T) {
	for _, vcs := range []DiggerVCSType{DiggerVCSGithub, DiggerVCSGitlab, DiggerVCSBitbucket} {
		for _, terminal := range []scheduler.DiggerJobStatus{scheduler.DiggerJobSucceeded, scheduler.DiggerJobFailed} {
			t.Run(string(vcs)+"/"+terminal.ToString(), func(t *testing.T) {
				t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
				db := newCommandOriginDB(t)
				require.NoError(t, db.GormDB.AutoMigrate(&Repo{}, &DiggerJobParentLink{}, &GithubDiggerJobLink{}))
				batch := newCommandOriginBatch(t, db, uuid.New())
				batch.VCS = vcs
				require.NoError(t, db.GormDB.Save(&batch).Error)
				org := setCommandOriginScope(t, db, batch.GithubInstallationId)
				require.NoError(t, db.GormDB.Create(&Repo{Name: "legacy", RepoFullName: batch.RepoFullName, OrganisationID: org}).Error)
				job, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "parent")
				require.NoError(t, err)
				require.Zero(t, job.DispatchProtocolVersion)
				started := *job
				started.Status, started.StatusUpdatedAt = scheduler.DiggerJobStarted, time.Date(2026, 11, 12, 6, 0, 1, 0, time.UTC)
				// An upgraded legacy row can have callback time before it has a
				// revision. That timestamp must still reject older completion.
				require.NoError(t, db.UpdateDiggerJob(&started))
				stale := started
				stale.Status, stale.StatusUpdatedAt = terminal, started.StatusUpdatedAt.Add(-time.Second)
				_, err = db.AcceptDiggerJobCallback(&stale, org)
				require.ErrorIs(t, err, ErrDiggerJobTransitionConflict)
				_, err = db.AcceptDiggerJobCallback(&started, org)
				require.NoError(t, err)
				finished := started
				finished.Status, finished.StatusUpdatedAt = terminal, started.StatusUpdatedAt.Add(time.Second)
				accepted, err := db.AcceptDiggerJobCallback(&finished, org)
				require.NoError(t, err, "unqualified unknown lineage must preserve legacy terminal reports")
				require.Equal(t, int64(2), accepted.CallbackRevision)
				require.Zero(t, accepted.WorkflowRunID)
				require.Nil(t, accepted.DependencyRetiredAt)
				require.Empty(t, accepted.DependencySkipReason())
				_, err = db.AcceptDiggerJobCallback(&started, org)
				require.ErrorIs(t, err, ErrDiggerJobTransitionConflict, "legacy acceptance must retain timestamp/revision checks")
				require.ErrorIs(t, db.UpdateDiggerJob(&started), ErrDiggerJobTransitionConflict)
				_, err = db.AcceptDiggerJobCallback(&finished, org+1000)
				require.ErrorIs(t, err, ErrDiggerJobCallbackForbidden)
			})
		}
	}
}

func TestDependencyQualificationPersistsAcrossReplicasPostgres(t *testing.T) {
	db, batch, parent, child := dependencyPostgresFixture(t)
	parent.WorkflowRunID, parent.WorkflowRunAttempt = 0, 0
	require.NoError(t, db.UpdateDiggerJob(&parent))
	require.Zero(t, parent.DispatchProtocolVersion, "old parent must still inherit strict batch handling")
	require.Equal(t, 1, child.DispatchProtocolVersion)
	orgID := setCommandOriginScope(t, db, batch.GithubInstallationId)
	require.NoError(t, db.GormDB.Create(&Repo{Name: "replicas", RepoFullName: batch.RepoFullName, OrganisationID: orgID}).Error)
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	actor, _ := commandOriginPostgresActor(t, db)
	legacy := parent
	legacy.StatusUpdatedAt = time.Date(2026, 11, 12, 6, 0, 1, 0, time.UTC)
	_, err := actor.AcceptDiggerJobCallback(&legacy, orgID)
	require.ErrorIs(t, err, ErrDiggerJobTransitionConflict, "disabled replica must honor another job's persisted qualification")
	require.Zero(t, dependencySavedJob(t, db, parent.ID).CallbackRevision)
	// A distinct new batch stays legacy; disabling cannot retrofit existing rows.
	nextBatch := newCommandOriginBatch(t, db, uuid.New())
	next, err := actor.CreateDiggerJob(nextBatch.ID, parent.SerializedJobSpec, "workflow.yml", nil, nil, "lazy", "new-unqualified")
	require.NoError(t, err)
	require.Zero(t, next.DispatchProtocolVersion)
	require.Zero(t, dependencySavedJob(t, db, parent.ID).DispatchProtocolVersion)
	require.Equal(t, 1, dependencySavedJob(t, db, child.ID).DispatchProtocolVersion)
	legitimate := legacy
	legitimate.WorkflowRunID, legitimate.WorkflowRunAttempt = 42665476673, 1
	_, err = actor.AcceptDiggerJobCallback(&legitimate, orgID)
	require.NoError(t, err)
	require.NoError(t, actor.UpdateBatchStatus(&batch))
	retired := dependencySavedJob(t, db, child.ID)
	require.NotNil(t, retired.DependencyRetiredAt)
	fence := *retired.DependencyRetiredAt
	claimed, err := actor.ClaimDiggerJobDispatch(&child)
	require.NoError(t, err)
	require.False(t, claimed)
	_, err = actor.AcceptDiggerJobCallback(&child, orgID)
	require.ErrorIs(t, err, ErrDiggerJobRetired)
	require.NoError(t, actor.UpdateBatchStatus(&batch))
	require.Equal(t, fence, *dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
}

func TestDependencyQualificationUnqualifiedKnownAttemptCannotBeCleared(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	db := newCommandOriginDB(t)
	require.NoError(t, db.GormDB.AutoMigrate(&Repo{}))
	batch := newCommandOriginBatch(t, db, uuid.New())
	org := setCommandOriginScope(t, db, batch.GithubInstallationId)
	require.NoError(t, db.GormDB.Create(&Repo{Name: "known", RepoFullName: batch.RepoFullName, OrganisationID: org}).Error)
	job, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "parent")
	require.NoError(t, err)
	started := *job
	started.Status, started.WorkflowRunID, started.WorkflowRunAttempt = scheduler.DiggerJobStarted, 42665476673, 2
	started.StatusUpdatedAt = time.Date(2026, 11, 12, 6, 0, 1, 0, time.UTC)
	_, err = db.AcceptDiggerJobCallback(&started, org)
	require.NoError(t, err)
	for _, attempt := range []int64{0, 1} {
		stale := started
		stale.Status, stale.WorkflowRunAttempt = scheduler.DiggerJobFailed, attempt
		stale.StatusUpdatedAt = started.StatusUpdatedAt.Add(time.Second)
		if attempt == 0 {
			stale.WorkflowRunID = 0
		}
		_, err = db.AcceptDiggerJobCallback(&stale, org)
		require.ErrorIs(t, err, ErrDiggerJobTransitionConflict)
	}
	saved := dependencySavedJob(t, db, job.ID)
	require.Equal(t, scheduler.DiggerJobStarted, saved.Status)
	require.Equal(t, int64(2), saved.WorkflowRunAttempt)
	require.Zero(t, saved.DispatchProtocolVersion)
}

func TestDependencyQualificationLegacyCompletionCannotAuthorizeRetirement(t *testing.T) {
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "false")
	db := newCommandOriginDB(t)
	require.NoError(t, db.GormDB.AutoMigrate(&Repo{}, &DiggerJobParentLink{}, &GithubDiggerJobLink{}))
	batch := newCommandOriginBatch(t, db, uuid.New())
	org := setCommandOriginScope(t, db, batch.GithubInstallationId)
	require.NoError(t, db.GormDB.Create(&Repo{Name: "unknown", RepoFullName: batch.RepoFullName, OrganisationID: org}).Error)
	parent, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "parent")
	require.NoError(t, err)
	parent.Status, parent.StatusUpdatedAt = scheduler.DiggerJobFailed, time.Date(2026, 11, 12, 6, 0, 1, 0, time.UTC)
	_, err = db.AcceptDiggerJobCallback(parent, org)
	require.NoError(t, err)
	// Even a later producer admission must not retroactively give that
	// unknown-generation completion authority for an irreversible fence.
	t.Setenv("DIGGER_DEPENDENCY_RETIREMENT_QUALIFICATION_ENABLED", "true")
	child, err := db.CreateDiggerJob(batch.ID, []byte(`{}`), "workflow.yml", nil, nil, "lazy", "child")
	require.NoError(t, err)
	require.Equal(t, 1, child.DispatchProtocolVersion)
	require.NoError(t, db.CreateDiggerJobParentLink(parent.DiggerJobID, child.DiggerJobID))
	result, err := db.ReconcileFailedDependencies(batch.ID)
	require.NoError(t, err)
	require.Empty(t, result.RetiredJobs)
	require.Nil(t, dependencySavedJob(t, db, child.ID).DependencyRetiredAt)
	require.NotEqual(t, scheduler.BatchJobFailed, result.BatchStatus)
}
