package telemetryarchive

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportRejectsUnregisteredInputBeforeAccessingArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "D1.parquet")
	if err := os.WriteFile(path, []byte("different source"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := ImportSummit(context.Background(), nil, path)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "sha256") {
		t.Fatalf("expected source SHA256 mismatch, got %v", err)
	}
}
