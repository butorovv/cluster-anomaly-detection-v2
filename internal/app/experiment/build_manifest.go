package experiment

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

const buildManifestName = "build-manifest.json"

var experimentBinaries = [...]string{"runner", "master", "worker", "replayer"}

type buildManifest struct {
	SchemaVersion int               `json:"schema_version"`
	SourceCommit  string            `json:"source_commit"`
	ArchiveSHA256 string            `json:"source_archive_sha256"`
	OverlaySHA256 string            `json:"overlay_sha256,omitempty"`
	BinarySHA256  map[string]string `json:"binary_sha256"`
}

type verifiedBuild struct {
	buildManifest
	manifestPath string
	manifestSHA  string
	paths        map[string]string
}

func validDigest(value string, widths ...int) bool {
	for _, width := range widths {
		if len(value) == width && strings.ToLower(value) == value {
			_, err := hex.DecodeString(value)
			return err == nil
		}
	}
	return false
}

func sourceArchive(path, expectedSHA string) ([]byte, string, error) {
	if !validDigest(expectedSHA, 64) {
		return nil, "", fmt.Errorf("a pinned source archive SHA-256 is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	if domain.Hash(data) != expectedSHA {
		return nil, "", fmt.Errorf("source archive SHA-256 mismatch")
	}
	// git archive of a commit records its identity in the global PAX header.
	// Reading the pinned archive never consults a checkout or its parent Git.
	header, err := tar.NewReader(bytes.NewReader(data)).Next()
	if err != nil || header.Typeflag != tar.TypeXGlobalHeader {
		return nil, "", fmt.Errorf("source archive must be a git archive of a commit")
	}
	commit := header.PAXRecords["comment"]
	if !validDigest(commit, 40, 64) {
		return nil, "", fmt.Errorf("source archive lacks a valid commit identifier")
	}
	return data, commit, nil
}

func sourcePath(root, name string) (string, error) {
	local, err := filepath.Localize(name)
	if err != nil || name == "." || strings.ContainsAny(name, "\\:") {
		return "", fmt.Errorf("invalid source entry %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if strings.EqualFold(part, ".git") {
			return "", fmt.Errorf("Git metadata is not a build input: %q", name)
		}
	}
	return filepath.Join(root, local), nil
}

func unpackSource(root string, data []byte) error {
	r := tar.NewReader(bytes.NewReader(data))
	// sourceArchive has already verified the leading global header.
	if _, err := r.Next(); err != nil {
		return err
	}
	for {
		h, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		path, err := sourcePath(root, strings.TrimSuffix(h.Name, "/"))
		if err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(f, r)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("unsupported source archive entry %q", h.Name)
		}
	}
}

func readOverlay(root string) (map[string][]byte, string, error) {
	files := map[string][]byte{}
	hashes := map[string]string{}
	err := fs.WalkDir(os.DirFS(root), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || name == "." {
			return err
		}
		path, err := sourcePath(root, name)
		if err != nil || entry.IsDir() {
			return err
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("overlay entry %q is not a regular file", name)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[name] = data
		hashes[name] = domain.Hash(data)
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if len(files) == 0 {
		return nil, "", fmt.Errorf("source overlay is empty")
	}
	canonical, err := json.Marshal(hashes)
	if err != nil {
		return nil, "", err
	}
	return files, domain.Hash(canonical), nil
}

func binaryPath(root, name string) string {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(root, "bin", name)
}

func buildFromArchive(ctx context.Context, archive, archiveSHA, overlay, out string) (string, error) {
	data, commit, err := sourceArchive(archive, archiveSHA)
	if err != nil {
		return "", err
	}
	var overlayFiles map[string][]byte
	manifest := buildManifest{SchemaVersion: 1, SourceCommit: commit, ArchiveSHA256: archiveSHA, BinarySHA256: map[string]string{}}
	if overlay != "" {
		overlayFiles, manifest.OverlaySHA256, err = readOverlay(overlay)
		if err != nil {
			return "", err
		}
	}
	root, err := filepath.Abs(out)
	if err != nil {
		return "", err
	}
	if err := os.Mkdir(root, 0700); err != nil {
		return "", fmt.Errorf("new build bundle directory required: %w", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.tar"), data, 0600); err != nil {
		return "", err
	}
	source := filepath.Join(root, "source")
	if err := unpackSource(source, data); err != nil {
		return "", err
	}
	for name, content := range overlayFiles {
		// Names have already passed sourcePath; unpackSource accepts no links.
		for _, dir := range []string{source, filepath.Join(root, "overlay")} {
			path := filepath.Join(dir, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return "", err
			}
			if err := os.WriteFile(path, content, 0600); err != nil {
				return "", err
			}
		}
	}
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-buildvcs=false", "-trimpath", "-o", bin+string(filepath.Separator),
		"./experiments/runner", "./cmd/master", "./cmd/worker", "./cmd/replayer")
	cmd.Dir = source
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOENV=off", "GOFLAGS=", "GOOS="+runtime.GOOS, "GOARCH="+runtime.GOARCH)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build pinned source: %w\n%s", err, output)
	}
	for _, name := range experimentBinaries {
		digest, err := fileSHA(binaryPath(root, name))
		if err != nil {
			return "", err
		}
		manifest.BinarySHA256[name] = digest
	}
	path := filepath.Join(root, buildManifestName)
	if err := saveJSON(path, manifest); err != nil {
		return "", err
	}
	return fileSHA(path)
}

func loadBuild(path, manifestSHA, runningExecutable string) (verifiedBuild, error) {
	if path == "" || !validDigest(manifestSHA, 64) {
		return verifiedBuild{}, fmt.Errorf("--build-manifest and --build-manifest-sha256 are required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return verifiedBuild{}, err
	}
	data, err := os.ReadFile(absolute)
	if err != nil {
		return verifiedBuild{}, err
	}
	if domain.Hash(data) != manifestSHA {
		return verifiedBuild{}, fmt.Errorf("build manifest SHA-256 mismatch")
	}
	build := verifiedBuild{manifestPath: absolute, manifestSHA: manifestSHA, paths: map[string]string{}}
	if err := domain.StrictJSON(data, &build.buildManifest); err != nil {
		return verifiedBuild{}, err
	}
	if build.SchemaVersion != 1 || len(build.BinarySHA256) != len(experimentBinaries) {
		return verifiedBuild{}, fmt.Errorf("unsupported or incomplete build manifest")
	}
	root := filepath.Dir(absolute)
	_, commit, err := sourceArchive(filepath.Join(root, "source.tar"), build.ArchiveSHA256)
	if err != nil {
		return verifiedBuild{}, err
	}
	if commit != build.SourceCommit {
		return verifiedBuild{}, fmt.Errorf("build source commit does not match the pinned archive")
	}
	if build.OverlaySHA256 != "" {
		_, digest, err := readOverlay(filepath.Join(root, "overlay"))
		if err != nil {
			return verifiedBuild{}, fmt.Errorf("read build overlay: %w", err)
		}
		if digest != build.OverlaySHA256 {
			return verifiedBuild{}, fmt.Errorf("build overlay fingerprint mismatch")
		}
	} else if _, err := os.Lstat(filepath.Join(root, "overlay")); !os.IsNotExist(err) {
		return verifiedBuild{}, fmt.Errorf("unrecorded build overlay")
	}
	for _, name := range experimentBinaries {
		build.paths[name] = binaryPath(root, name)
	}
	build.paths["runner"] = runningExecutable
	for _, name := range experimentBinaries {
		if err := build.verifyBinary(name); err != nil {
			return verifiedBuild{}, err
		}
	}
	return build, nil
}

func (b verifiedBuild) verifyBinary(name string) error {
	actual, err := fileSHA(b.paths[name])
	if err != nil {
		return fmt.Errorf("verify %s binary: %w", name, err)
	}
	if actual != b.BinarySHA256[name] {
		return fmt.Errorf("%s binary SHA-256 mismatch", name)
	}
	return nil
}

func (b verifiedBuild) verifySaved(saved Result) error {
	if saved.BuildManifestSHA != b.manifestSHA || saved.Environment["source_commit"] != b.SourceCommit ||
		saved.Environment["source_archive_sha256"] != b.ArchiveSHA256 {
		return fmt.Errorf("saved result %q lacks matching build evidence: use a new output directory", saved.Spec.Name)
	}
	overlay, _ := saved.Environment["overlay_sha256"].(string)
	if overlay != b.OverlaySHA256 || len(saved.BinarySHA256) != len(b.BinarySHA256) {
		return fmt.Errorf("saved result %q has different build inputs: use a new output directory", saved.Spec.Name)
	}
	for _, name := range experimentBinaries {
		if saved.BinarySHA256[name] != b.BinarySHA256[name] {
			return fmt.Errorf("saved result %q has a different %s binary", saved.Spec.Name, name)
		}
	}
	return nil
}

// Check every saved run before touching the matrix or any per-run artifact.
func checkResumeBuild(cfg Config, build verifiedBuild) error {
	data, err := os.ReadFile(filepath.Join(cfg.Output, "matrix.json"))
	if err == nil {
		var previous []Result
		if err := json.Unmarshal(data, &previous); err != nil {
			return err
		}
		for _, saved := range previous {
			if err := build.verifySaved(saved); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, spec := range cfg.Runs {
		data, err := os.ReadFile(filepath.Join(cfg.Output, spec.Name+".json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var saved Result
		if err := json.Unmarshal(data, &saved); err != nil {
			return err
		}
		if err := build.verifySaved(saved); err != nil {
			return err
		}
	}
	return nil
}
