package git_utils

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func installFakeGit(t *testing.T, script string) {
	t.Helper()
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "git"), []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestCloneUsesConfigTimeoutForEachCommand(t *testing.T) {
	// Each command fits within two seconds, but together they exceed it.
	installFakeGit(t, "echo \"$1\" >> \"$GIT_TEST_COMMANDS\"\nexec sleep 1.1\n")
	commandsPath := filepath.Join(t.TempDir(), "commands")
	t.Setenv("GIT_TEST_COMMANDS", commandsPath)
	config := []byte("git_timeout: 2\n")
	readFile := func(ctx context.Context, path, ref string) ([]byte, error) {
		if path != "digger.yml" || ref != "commit" {
			t.Fatalf("unexpected config lookup: %s at %s", path, ref)
		}
		if _, err := os.Stat(commandsPath); !os.IsNotExist(err) {
			t.Fatal("Git started before reading configuration")
		}
		return config, nil
	}
	var clonedDir string
	err := CloneGitRepoAndDoActionWithConfig("https://example.com/repo", "main", "commit", "", "", readFile, func(dir string) error {
		clonedDir = dir
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if clonedDir == "" {
		t.Fatal("repository action was not called")
	}
	if _, err := os.Stat(clonedDir); !os.IsNotExist(err) {
		t.Fatalf("clone directory was not removed: %v", err)
	}
	commands, err := os.ReadFile(commandsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(commands) != "clone\ncheckout\n" {
		t.Fatalf("unexpected commands: %q", commands)
	}

	// The same clone must fail with a shorter YAML timeout.
	if err := os.Remove(commandsPath); err != nil {
		t.Fatal(err)
	}
	config = []byte("git_timeout: 1\n")
	err = CloneGitRepoAndDoActionWithConfig("https://example.com/repo", "main", "commit", "", "", readFile, func(string) error {
		t.Fatal("action called after timeout")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func TestExplicitCloneTimeoutAndCleanup(t *testing.T) {
	installFakeGit(t, "exec sleep 10\n")
	cloneRoot := t.TempDir()
	t.Setenv("TMPDIR", cloneRoot)
	err := CloneGitRepoAndDoActionWithTimeout("https://example.com/repo", "main", "", "", "", 20*time.Millisecond, func(string) error {
		t.Fatal("action called after timeout")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}
	if !strings.Contains(err.Error(), "timed out after 20ms") {
		t.Fatalf("timeout duration missing: %v", err)
	}
	entries, err := os.ReadDir(cloneRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed clone left temporary files: %v", entries)
	}
}

func TestGitCommandFailureIsNotTimeout(t *testing.T) {
	installFakeGit(t, "echo 'clone failed' >&2\nexit 1\n")
	_, err := NewGitShell(t.TempDir(), nil).runCommand("clone")
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected process failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "clone failed") {
		t.Fatalf("command stderr missing: %v", err)
	}
}

func TestCloneFetchesCommitTheBranchMovedPast(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = repo
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-b", "feature")
	for _, content := range []string{"old\n", "new\n"} {
		if err := os.WriteFile(filepath.Join(repo, "digger.yml"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-m", content)
	}
	old := git("rev-parse", "HEAD~1")

	var got string
	err := CloneGitRepoAndDoActionWithTimeout("file://"+repo, "feature", old, "", "", time.Minute, func(dir string) error {
		b, err := os.ReadFile(filepath.Join(dir, "digger.yml"))
		got = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "old\n" {
		t.Fatalf("checked out %q, want the requested commit", got)
	}
}
