package group

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
)

type RBI2Sink struct {
	Raw      func(Raw) error
	Temporal func(RBI2TemporalResult) error
	Evidence func(RBI2Evidence) error
	Event    func(RBI2Event) error
}

type RBI2OfflineOptions struct {
	Files        BundleFiles
	Input        string
	Manifest     string
	Window       analysis.Window
	Groups       int
	DeltaSeconds int
	Sink         RBI2Sink
}

type RBI2RunSummary struct {
	Identity          RBI2Identity  `json:"identity"`
	Binding           local.Binding `json:"binding"`
	Ticks             int           `json:"group_ticks"`
	PositiveTicks     int           `json:"positive_ticks"`
	Events            int           `json:"events"`
	NewEvents         int           `json:"new_events"`
	RBIntervals       int           `json:"rb_intervals"`
	RBSupportedEvents int           `json:"rb_supported_events"`
}

// SummitRBI2Files maps a caller's physical assets root to the unchanged frozen
// logical keys. profileSHA is the caller's trusted, independently obtained pin.
func SummitRBI2Files(root, profileSHA string) BundleFiles {
	asset := func(key string) BundleFile {
		return BundleFile{Path: filepath.Join(root, filepath.FromSlash(key)), InputKey: key}
	}
	return BundleFiles{
		ProfilePath: filepath.Join(root, "configs", "group-profile.summit-v1.json"), ProfileSHA256: profileSHA,
		PreparedKey: "data/prepared/summit-D1-pilot/D1.parquet", ManifestKey: "data/prepared/summit-D1-pilot/manifest.json",
		Statistical: asset("results/statistical-20220118-8h-v1.json"), Isolation: asset("results/isolation-20220118-v1.json"),
		B1: asset("configs/local-b1.summit-20220118.json"), Hardware: asset("configs/summit-hardware-reference.json"), Config: asset("configs/group-detection.summit-v1.json"),
	}
}

// RunRBI2Offline uses the same EvaluateGroup/EvaluateI2 path as a future Worker.
// It trains nothing. Sinks receive every raw/temporal tick, positive evidence,
// and one parent event stream with nested R+B intervals.
func RunRBI2Offline(ctx context.Context, options RBI2OfflineOptions) (RBI2RunSummary, error) {
	var summary RBI2RunSummary
	if ctx == nil || options.Sink.Raw == nil || options.Sink.Temporal == nil || options.Sink.Evidence == nil || options.Sink.Event == nil {
		return summary, fmt.Errorf("context and all R/B/I2 sinks required")
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	if options.Groups != 4 && options.Groups != 8 && options.Groups != 16 {
		return summary, fmt.Errorf("groups must be 4, 8 or 16")
	}
	if !options.Window.Valid() || !options.Window.Start.Equal(options.Window.Start.Truncate(analysis.Step)) || !options.Window.End.Equal(options.Window.End.Truncate(analysis.Step)) || (options.DeltaSeconds != 0 && options.DeltaSeconds != 10 && options.DeltaSeconds != 30 && options.DeltaSeconds != 60) {
		return summary, fmt.Errorf("grid window and delta 0/10/30/60 required")
	}
	bundle, err := LoadBundle(options.Files)
	if err != nil {
		return summary, err
	}
	identity, err := NewRBI2Identity(bundle.profileSHA, bundle.profile.InputSHA, options.Groups, options.Window, options.DeltaSeconds)
	if err != nil {
		return summary, err
	}
	source, err := bundle.OpenSource(options.Input, options.Manifest)
	if err != nil {
		return summary, err
	}
	summary.Identity, summary.Binding = identity, cloneRBI2Binding(bundle.profile.Binding)
	profiles := bundle.profile.Groups[:options.Groups]
	journals := make([]*RBI2Journal, options.Groups)
	for i, p := range profiles {
		journals[i], err = NewRBI2Journal(identity, p.Group, p.Hosts)
		if err != nil {
			return summary, err
		}
	}
	writeEvents := func(events []RBI2Event) error {
		for _, event := range events {
			if err := options.Sink.Event(event); err != nil {
				return err
			}
			summary.Events++
			summary.RBIntervals += len(event.RBSubspans)
			if event.NewDisjointFromRB {
				summary.NewEvents++
			} else {
				summary.RBSupportedEvents++
			}
		}
		return nil
	}
	err = WalkTicks(ctx, source, options.Window, func(tick Tick) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows := make([]Raw, options.Groups)
		for i, p := range profiles {
			row, err := bundle.EvaluateGroup(p.Group, tick, nil)
			if err != nil {
				return err
			}
			rows[i] = row
		}
		AnnotateOutside(rows) // Diagnostic only; does not change any vote.
		for i, raw := range rows {
			tr, evidence, events, err := journals[i].Step(raw)
			if err != nil {
				return err
			}
			if err = options.Sink.Raw(raw); err != nil {
				return err
			}
			if err = options.Sink.Temporal(tr); err != nil {
				return err
			}
			if evidence != nil {
				if err = options.Sink.Evidence(*evidence); err != nil {
					return err
				}
			}
			if err = writeEvents(events); err != nil {
				return err
			}
			summary.Ticks++
			if tr.Vote == Positive {
				summary.PositiveTicks++
			}
		}
		return nil
	})
	if err != nil {
		return summary, err
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	for _, journal := range journals {
		events, err := journal.Flush(options.Window.End)
		if err != nil {
			return summary, err
		}
		if err = writeEvents(events); err != nil {
			return summary, err
		}
	}
	// All inputs remain immutable, including telemetry and the model pin, for the
	// full run. Recheck bytes after inference before publishing a success summary.
	for _, asset := range []BundleFile{options.Files.Statistical, options.Files.Isolation, options.Files.B1, options.Files.Hardware, options.Files.Config,
		{Path: options.Input, InputKey: options.Files.PreparedKey}, {Path: options.Manifest, InputKey: options.Files.ManifestKey}} {
		if err = checkFileSHA(asset.Path, bundle.profile.InputSHA[asset.InputKey]); err != nil {
			return summary, err
		}
	}
	if err = checkFileSHA(options.Files.ProfilePath, bundle.profileSHA); err != nil {
		return summary, err
	}
	return summary, nil
}

func cloneRBI2Binding(binding local.Binding) local.Binding {
	binding.Hosts = append([]string(nil), binding.Hosts...)
	binding.FeatureOrder = append([]string(nil), binding.FeatureOrder...)
	return binding
}
