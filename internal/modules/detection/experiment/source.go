// Package experiment composes pure mathematics with the existing Parquet reader.
package experiment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
)

type Source struct {
	Path             string   `json:"prepared_path"`
	SHA256           string   `json:"prepared_sha256"`
	Hosts            []string `json:"hosts"`
	Groups           []string `json:"groups"`
	AssumedTimezone  string   `json:"assumed_timezone"`
	TimezoneVerified bool     `json:"timezone_verified"`
	hosts            map[string]bool
}

func OpenSource(path, manifest string) (*Source, error) {
	bytes, err := os.ReadFile(manifest)
	if err != nil {
		return nil, err
	}
	var m struct {
		Features         []string          `json:"feature_order"`
		Hosts            []string          `json:"selected_hosts"`
		Groups           []string          `json:"selected_groups"`
		Hashes           map[string]string `json:"prepared_file_hashes"`
		Schema           string            `json:"schema_version"`
		AssumedTimezone  string            `json:"assumed_timezone"`
		TimezoneVerified bool              `json:"timezone_verified"`
	}
	if err = json.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}
	if !slices.Equal(m.Features, domain.FeatureNames[:]) || m.Schema != parquet.PreparedSchemaVersion || len(m.Hosts) != 288 || len(m.Groups) != 16 {
		return nil, fmt.Errorf("manifest must preserve F18-v1 and the fixed 16 x 18 topology")
	}
	s := &Source{Path: path, Hosts: m.Hosts, Groups: m.Groups, AssumedTimezone: m.AssumedTimezone, TimezoneVerified: m.TimezoneVerified, hosts: map[string]bool{}}
	groups := map[string]map[int]bool{}
	for _, h := range s.Hosts {
		g, i, e := topology.ParseHostname(h)
		if e != nil {
			return nil, e
		}
		if s.hosts[h] {
			return nil, fmt.Errorf("duplicate manifest host")
		}
		s.hosts[h] = true
		if groups[g] == nil {
			groups[g] = map[int]bool{}
		}
		if groups[g][i] {
			return nil, fmt.Errorf("duplicate topology position")
		}
		groups[g][i] = true
	}
	if len(groups) != 16 {
		return nil, fmt.Errorf("manifest group mismatch")
	}
	seen := map[string]bool{}
	for _, g := range m.Groups {
		if seen[g] || len(groups[g]) != 18 {
			return nil, fmt.Errorf("manifest topology is not 16 full disjoint groups")
		}
		seen[g] = true
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, f)
	closeErr := f.Close()
	if err = errors.Join(copyErr, closeErr); err != nil {
		return nil, err
	}
	s.SHA256 = hex.EncodeToString(hash.Sum(nil))
	if expected := m.Hashes[filepath.Base(path)]; expected == "" || expected != s.SHA256 {
		return nil, fmt.Errorf("prepared checksum differs from manifest")
	}
	return s, nil
}

type Audit struct {
	Window                    analysis.Window `json:"window"`
	States                    int             `json:"states"`
	Valid                     int             `json:"valid_states"`
	Timestamps                int             `json:"timestamps"`
	Hosts                     int             `json:"hosts"`
	MinimumStatesPerTimestamp int             `json:"minimum_states_per_timestamp"`
}

func (s *Source) Scan(ctx context.Context, w analysis.Window, visit func(domain.TelemetryRecord) error) (audit Audit, err error) {
	if !w.Valid() {
		return audit, fmt.Errorf("invalid scan window")
	}
	raw, err := parquet.OpenParquet(s.Path)
	if err != nil {
		return audit, err
	}
	defer func() { err = errors.Join(err, raw.Close()) }()
	r := usecase.WindowReader{Source: raw, Start: w.Start, End: w.End}
	audit.Window = w
	hosts := map[string]bool{}
	counts := map[time.Time]int{}
	for {
		v, e := r.Next(ctx)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return audit, e
		}
		if !w.Contains(v.Timestamp) || !s.hosts[v.Hostname] {
			return audit, fmt.Errorf("state outside fixed window/topology")
		}
		if v.Timestamp.Sub(w.Start)%analysis.Step != 0 {
			return audit, fmt.Errorf("state outside 10-second grid")
		}
		audit.States++
		if v.Valid {
			audit.Valid++
		}
		hosts[v.Hostname] = true
		counts[v.Timestamp]++
		if visit != nil {
			if e = visit(v); e != nil {
				return audit, e
			}
		}
	}
	audit.Timestamps, audit.Hosts = len(counts), len(hosts)
	for _, n := range counts {
		if audit.MinimumStatesPerTimestamp == 0 || n < audit.MinimumStatesPerTimestamp {
			audit.MinimumStatesPerTimestamp = n
		}
	}
	return audit, nil
}

func (s *Source) TrainingData() analysis.Data {
	return func(ctx context.Context, visit func(analysis.Observation) error) error {
		_, err := s.Scan(ctx, analysis.NIRWindows()[0], func(v domain.TelemetryRecord) error {
			if !v.Valid {
				return nil
			}
			return visit(analysis.Observation{Timestamp: v.Timestamp, Features: v.Features()})
		})
		return err
	}
}
