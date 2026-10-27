package offline

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func callRun(t *testing.T, args ...string) error {
	t.Helper()
	return Run(context.Background(), args, io.Discard, io.Discard)
}

func TestRunValidatesFlagsBeforeFilesystemIO(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"missing required", nil},
		{"sha short", []string{"--assets", "missing", "--profile-sha256", "abc", "--output", "never-created"}},
		{"sha nonhex", []string{"--assets", "missing", "--profile-sha256", strings.Repeat("g", 64), "--output", "never-created"}},
		{"window", []string{"--assets", "missing", "--profile-sha256", testSHA, "--output", "never-created", "--window", "D3"}},
		{"groups", []string{"--assets", "missing", "--profile-sha256", testSHA, "--output", "never-created", "--groups", "3"}},
		{"delta", []string{"--assets", "missing", "--profile-sha256", testSHA, "--output", "never-created", "--delta", "5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := callRun(t, tc.args...); err == nil {
				t.Fatal("expected validation error")
			}
			if _, err := os.Stat("never-created"); !os.IsNotExist(err) {
				t.Fatalf("validation touched filesystem: stat error=%v", err)
			}
		})
	}
}

func TestRunWindowAndDeltaAndGroupFlagsAccepted(t *testing.T) {
	for _, tc := range []struct{ window, groups, delta string }{{"D1", "4", "0"}, {"D2", "8", "10"}, {"D2", "16", "30"}, {"D1", "16", "60"}} {
		out := t.TempDir()
		args := []string{"--assets", filepath.Join(out, "no-assets"), "--profile-sha256", testSHA, "--output", filepath.Join(out, "result"), "--window", tc.window, "--groups", tc.groups, "--delta", tc.delta}
		err := callRun(t, args...)
		if err == nil || strings.Contains(err.Error(), "--window") || strings.Contains(err.Error(), "--groups") || strings.Contains(err.Error(), "--delta") {
			t.Fatalf("valid options failed during flag validation: %v", err)
		}
	}
}

func TestRunRejectsExistingOutput(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "already")
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	err := callRun(t, "--assets", root, "--profile-sha256", testSHA, "--output", output)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("got %v, want existing output error", err)
	}
}

func TestRunRejectsOutputCreationError(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "parent", "child")
	err := callRun(t, "--assets", root, "--profile-sha256", testSHA, "--output", output)
	if err == nil || !strings.Contains(err.Error(), "create output directory") {
		t.Fatalf("got %v, want output creation error", err)
	}
}

func TestRunCancellationDoesNotWriteSummary(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "result")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	err := Run(ctx, []string{"--assets", root, "--profile-sha256", testSHA, "--output", output}, &stdout, io.Discard)
	if err == nil {
		t.Fatal("expected canceled run")
	}
	if _, statErr := os.Stat(filepath.Join(output, "summary.json")); !os.IsNotExist(statErr) {
		t.Fatalf("summary exists after canceled run: %v", statErr)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout contains summary after cancellation: %q", stdout.String())
	}
}
