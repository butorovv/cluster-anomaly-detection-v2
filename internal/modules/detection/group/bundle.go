package group

import (
	"fmt"
	"slices"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
)

// BundleFile separates a physical file location from its historical InputSHA
// key. Moving a file must not rewrite that logical key or its frozen profile.
type BundleFile struct {
	Path     string
	InputKey string
}

// BundleFiles is a caller-supplied, trusted reference to immutable assets.
// ProfileSHA256 is the trust anchor, not a checksum inferred from an untrusted
// profile. PreparedKey/ManifestKey name source hashes, not files to open at load.
type BundleFiles struct {
	ProfilePath   string
	ProfileSHA256 string
	PreparedKey   string
	ManifestKey   string
	Statistical   BundleFile
	Isolation     BundleFile
	B1            BundleFile
	Hardware      BundleFile
	Config        BundleFile
}

// Bundle owns validated, private model data. There is no training or telemetry
// scan during loading; actual replay data is verified separately by OpenSource.
type Bundle struct {
	profile     *Profile
	profileSHA  string
	preparedKey string
	manifestKey string
	local       *local.Detector
	hardware    *Hardware
}

func LoadBundle(files BundleFiles) (*Bundle, error) {
	if !validSHA(files.ProfileSHA256) {
		return nil, fmt.Errorf("trusted group profile SHA256 required")
	}
	if err := checkFileSHA(files.ProfilePath, files.ProfileSHA256); err != nil {
		return nil, err
	}
	p, err := LoadProfile(files.ProfilePath)
	if err != nil {
		return nil, err
	}
	assets := []BundleFile{files.Statistical, files.Isolation, files.B1, files.Hardware, files.Config}
	keys := map[string]bool{}
	for _, key := range []string{files.PreparedKey, files.ManifestKey} {
		if key == "" || keys[key] || !validSHA(p.InputSHA[key]) {
			return nil, fmt.Errorf("invalid or duplicate source input key")
		}
		keys[key] = true
	}
	for _, asset := range assets {
		if asset.InputKey == "" || keys[asset.InputKey] || !validSHA(p.InputSHA[asset.InputKey]) {
			return nil, fmt.Errorf("invalid or duplicate model input key")
		}
		keys[asset.InputKey] = true
		if err := checkFileSHA(asset.Path, p.InputSHA[asset.InputKey]); err != nil {
			return nil, err
		}
	}
	if len(keys) != len(p.InputSHA) || p.InputSHA[files.PreparedKey] != p.Binding.PreparedSHA256 || p.InputSHA[files.Statistical.InputKey] != p.Binding.StatisticalSHA256 || p.InputSHA[files.Isolation.InputKey] != p.Binding.IsolationSHA256 {
		return nil, fmt.Errorf("group profile input/binding mismatch")
	}
	ld, err := local.LoadFrozenDetector(files.Statistical.Path, files.Isolation.Path, files.B1.Path, p.Binding)
	if err != nil {
		return nil, err
	}
	hw, err := LoadHardware(files.Hardware.Path)
	if err != nil {
		return nil, err
	}
	if _, err := LoadConfig(files.Config.Path); err != nil {
		return nil, err
	}
	// As in the offline loader, inputs must remain immutable during loading.
	// Recheck bytes after validators which reopen files, before publishing models.
	for _, asset := range append(assets, BundleFile{Path: files.ProfilePath}) {
		want := p.InputSHA[asset.InputKey]
		if asset.InputKey == "" {
			want = files.ProfileSHA256
		}
		if err := checkFileSHA(asset.Path, want); err != nil {
			return nil, err
		}
	}
	return &Bundle{profile: p, profileSHA: files.ProfileSHA256, preparedKey: files.PreparedKey, manifestKey: files.ManifestKey, local: ld, hardware: hw}, nil
}

func checkFileSHA(path, expected string) error {
	actual, err := FileSHA(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("file SHA256 mismatch: %s", path)
	}
	return nil
}

// EvaluateGroup exposes only inference over the bundle's private profile/models.
// It does not advance an Engine or calculate outside-group support.
func (b *Bundle) EvaluateGroup(groupID string, tick Tick, timing *Timing) (Raw, error) {
	if b == nil || b.profile == nil {
		return Raw{}, fmt.Errorf("nil frozen bundle")
	}
	for i := range b.profile.Groups {
		if b.profile.Groups[i].Group == groupID {
			return EvaluateGroup(&b.profile.Groups[i], tick, b.hardware, b.local, timing)
		}
	}
	return Raw{}, fmt.Errorf("group not in frozen inventory: %s", groupID)
}

// RunID uses frozen logical paths regardless of the physical bundle locations.
func (b *Bundle) RunID(groups int, window analysis.Window) (string, error) {
	if b == nil || b.profile == nil {
		return "", fmt.Errorf("nil frozen bundle")
	}
	return RunID(b.profileSHA, b.profile.InputSHA, groups, window)
}

// OpenSource verifies actual telemetry and manifest bytes separately from model
// loading. This v1 path accepts the same historical source, not a new dataset.
func (b *Bundle) OpenSource(input, manifest string) (*experiment.Source, error) {
	if b == nil || b.profile == nil {
		return nil, fmt.Errorf("nil frozen bundle")
	}
	wantManifest := b.profile.InputSHA[b.manifestKey]
	if err := checkFileSHA(manifest, wantManifest); err != nil {
		return nil, err
	}
	s, err := experiment.OpenSource(input, manifest)
	if err != nil {
		return nil, err
	}
	groups := make([]string, len(b.profile.Groups))
	for i := range groups {
		groups[i] = b.profile.Groups[i].Group
	}
	binding := b.profile.Binding
	if s.SHA256 != b.profile.InputSHA[b.preparedKey] || !slices.Equal(s.Hosts, binding.Hosts) || !slices.Equal(s.Groups, groups) || s.AssumedTimezone != binding.AssumedTimezone || s.TimezoneVerified != binding.TimezoneVerified {
		return nil, fmt.Errorf("replay source differs from frozen bundle")
	}
	if err := checkFileSHA(input, b.profile.InputSHA[b.preparedKey]); err != nil {
		return nil, err
	}
	if err := checkFileSHA(manifest, wantManifest); err != nil {
		return nil, err
	}
	return s, nil
}
