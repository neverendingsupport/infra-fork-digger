package spec

import (
	"errors"
	"testing"

	"github.com/diggerhq/digger/libs/ci"
	"github.com/stretchr/testify/assert"
)

type headPullRequestService struct {
	ci.MockPullRequestManager
	head string
	err  error
}

func (s headPullRequestService) GetBranchName(int) (string, string, string, string, error) {
	return "feature", s.head, "main", "", s.err
}

func TestCheckPullRequestHeadAcceptsCurrentHead(t *testing.T) {
	assert.NoError(t, checkPullRequestHead(headPullRequestService{head: "bbb"}, 7, "bbb"))
}

func TestCheckPullRequestHeadRejectsStaleCommit(t *testing.T) {
	err := checkPullRequestHead(headPullRequestService{head: "bbb"}, 7, "aaa")
	assert.ErrorContains(t, err, "PR #7 is now at commit bbb, but this job was created for commit aaa")
}

func TestCheckPullRequestHeadSkipsUnknownHead(t *testing.T) {
	assert.NoError(t, checkPullRequestHead(headPullRequestService{}, 7, "aaa"))
}

func TestCheckPullRequestHeadFailsWhenHeadIsUnreadable(t *testing.T) {
	err := checkPullRequestHead(headPullRequestService{err: errors.New("boom")}, 7, "aaa")
	assert.ErrorContains(t, err, "could not read the head commit of PR #7")
}

func TestCommitPattern(t *testing.T) {
	assert.True(t, commitPattern.MatchString("02a31cd4e15e47f86c3f8727b98b8abe844d9a50"))
	for _, bad := range []string{"", "main", "../../x", "--upload-pack=x", "02A31CD"} {
		assert.False(t, commitPattern.MatchString(bad), bad)
	}
}
