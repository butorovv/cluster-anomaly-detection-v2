package offline

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This supplements output parity: changes to even unexercised numerical paths
// must be explicit. Import relocation and the moved hostname parser are the only
// allowed differences in group/local/analysis declarations.
func TestV1ScientificDeclarationsUnchanged(t *testing.T) {
	source := os.Getenv("OFFLINE_V1_SOURCE")
	if source == "" {
		t.Skip("OFFLINE_V1_SOURCE required for pinned-source declaration comparison")
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "docs/migration/02-provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Files []struct {
			Source string `json:"source"`
			SHA    string `json:"source_sha256"`
			Target string `json:"target"`
		}
	}
	if err = json.Unmarshal(b, &manifest); err != nil {
		t.Fatal(err)
	}
	declarations := func(b []byte) string {
		t.Helper()
		b = bytes.ReplaceAll(b, []byte("topology.ParseHostname"), []byte("domain.ParseHostname"))
		b = bytes.ReplaceAll(b, []byte("../../../../configs/"), []byte("../../configs/"))
		set := token.NewFileSet()
		f, err := parser.ParseFile(set, "kernel.go", b, 0)
		if err != nil {
			t.Fatal(err)
		}
		var result bytes.Buffer
		for _, decl := range f.Decls {
			if g, ok := decl.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
				continue
			}
			if err := format.Node(&result, set, decl); err != nil {
				t.Fatal(err)
			}
		}
		return result.String()
	}
	compared := 0
	for _, f := range manifest.Files {
		if !strings.HasPrefix(f.Source, "internal/group/") && !strings.HasPrefix(f.Source, "internal/local/") && !strings.HasPrefix(f.Source, "internal/analysis/") {
			continue
		}
		original, err := os.ReadFile(filepath.Join(source, filepath.FromSlash(f.Source)))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(original)) != f.SHA {
			t.Fatalf("source pin mismatch: %s", f.Source)
		}
		moved, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f.Target)))
		if err != nil {
			t.Fatal(err)
		}
		if declarations(original) != declarations(moved) {
			t.Fatalf("scientific declarations changed: %s", f.Source)
		}
		compared++
	}
	if compared < 40 {
		t.Fatalf("incomplete scientific inventory: %d", compared)
	}
	t.Logf("%d Go source/test files: identical declarations after import/parser/path relocation", compared)
}
