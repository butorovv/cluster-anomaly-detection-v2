package telemetryarchive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
	"github.com/jackc/pgx/v5"
)

const SummitSHA = "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7"
const SummitAsset = "data/prepared/summit-D1-pilot/D1.parquet"
const SummitStart = "2022-01-18T08:00:00Z"
const SummitEnd = "2022-01-18T16:00:00Z"

var summitWindow = domain.InputWindow{
	Start: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC),
	End:   time.Date(2022, 1, 18, 16, 0, 0, 0, time.UTC),
}

type Manifest struct {
	SourceSHA     string    `json:"source_sha256"`
	FeatureSchema string    `json:"feature_schema"`
	TopologySHA   string    `json:"topology_sha256"`
	Start         time.Time `json:"window_start"`
	End           time.Time `json:"window_end"`
	Step          int       `json:"step_seconds"`
	Groups        int       `json:"groups"`
	Hosts         int       `json:"hosts"`
	Observations  int64     `json:"observations"`
	Measurements  int64     `json:"measurements"`
	VerifiedAt    time.Time `json:"verified_at"`
	Reused        bool      `json:"reused"`
}

func TopologySHA() string {
	encoded, _ := json.Marshal(topology.CanonicalGroups())
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func registerSource(ctx context.Context, conn *pgx.Conn, sha string) error {
	// Claim an empty archive before any measurements are written. Never adopt
	// untracked rows from a database with an unknown physical source.
	_, err := conn.Exec(ctx, `INSERT INTO cad_archive.source(sha256,feature_schema,topology_sha256,step_seconds)
 SELECT $1,$2,$3,10 WHERE NOT EXISTS (SELECT 1 FROM project.telemetry)
 ON CONFLICT (singleton) DO NOTHING`, sha, parquet.PreparedSchemaVersion, TopologySHA())
	if err != nil {
		return err
	}
	var stored, schema, topologyHash string
	if err := conn.QueryRow(ctx, `SELECT sha256,feature_schema,topology_sha256 FROM cad_archive.source`).
		Scan(&stored, &schema, &topologyHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("archive contains measurements with unverified provenance")
		}
		return err
	}
	if stored != sha || schema != parquet.PreparedSchemaVersion || topologyHash != TopologySHA() {
		return fmt.Errorf("archive database belongs to a different source, F18 version or topology")
	}
	return nil
}

func ImportSummit(ctx context.Context, conn *pgx.Conn, path string) (Manifest, error) {
	if _, err := db.ApplyArchive(ctx, conn); err != nil {
		return Manifest{}, err
	}
	// Held for the import, including verification. The connection is dedicated
	// to this invocation so WriteBatch can use it without waiting for a pool.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(726249201, 2)`); err != nil {
		return Manifest{}, err
	}
	defer func() {
		release, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(release, `SELECT pg_advisory_unlock(726249201, 2)`)
	}()
	if err := registerSource(ctx, conn, SummitSHA); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{SourceSHA: SummitSHA, FeatureSchema: parquet.PreparedSchemaVersion,
		TopologySHA: TopologySHA(), Step: 10, Start: summitWindow.Start, End: summitWindow.End}
	err := conn.QueryRow(ctx, `SELECT groups,hosts,observations,measurements,verified_at
 FROM cad_archive.imports WHERE source_sha256=$1 AND window_start=$2 AND window_end=$3`,
		SummitSHA, summitWindow.Start, summitWindow.End).
		Scan(&manifest.Groups, &manifest.Hosts, &manifest.Observations, &manifest.Measurements, &manifest.VerifiedAt)
	if err == nil {
		manifest.Reused = true
		return manifest, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Manifest{}, err
	}

	reader, _, err := parquet.OpenVerifiedParquet(ctx, path, SummitSHA)
	if err != nil {
		return Manifest{}, err
	}
	writer, err := archive.New(ctx, conn, 1024)
	if err != nil {
		return Manifest{}, errors.Join(err, reader.Close())
	}
	batches := &importBatches{writer: writer, rows: make([]domain.TelemetryRecord, 0, 1024)}
	window := &usecase.WindowReader{Source: reader, Start: summitWindow.Start, End: summitWindow.End}
	stats, err := usecase.ReplayFullGrid(ctx, window, batches, batches, usecase.RealClock{}, 0, summitWindow.Start, summitWindow.End)
	if err = errors.Join(err, reader.Close()); err != nil {
		return Manifest{}, err
	}
	if err := batches.flush(ctx); err != nil {
		return Manifest{}, err
	}
	const groups, hosts, ticks = 16, 288, 2880
	const records = hosts * ticks
	const measurements = records * domain.FeatureCount
	if stats.Rows != records || stats.Groups != groups || stats.Hosts != hosts || stats.Timestamps != ticks ||
		stats.Completions != groups*ticks || batches.measurements != measurements {
		return Manifest{}, fmt.Errorf("incomplete registered D2 input: %+v, measurements=%d", stats, batches.measurements)
	}
	var persisted int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM project.telemetry WHERE ts >= $1 AND ts < $2`,
		summitWindow.Start, summitWindow.End).Scan(&persisted); err != nil {
		return Manifest{}, err
	}
	if persisted != measurements {
		return Manifest{}, fmt.Errorf("stored D2 measurement count differs: %d", persisted)
	}
	manifest.Groups, manifest.Hosts, manifest.Observations, manifest.Measurements = groups, hosts, records, persisted
	err = conn.QueryRow(ctx, `INSERT INTO cad_archive.imports
 (source_sha256, window_start, window_end, groups, hosts, observations, measurements)
 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING verified_at`,
		manifest.SourceSHA, manifest.Start, manifest.End, manifest.Groups, manifest.Hosts,
		manifest.Observations, manifest.Measurements).Scan(&manifest.VerifiedAt)
	return manifest, err
}

type importBatches struct {
	writer       *archive.Writer
	rows         []domain.TelemetryRecord
	measurements int64
}

func (b *importBatches) Send(ctx context.Context, record domain.TelemetryRecord) error {
	b.rows = append(b.rows, record)
	if len(b.rows) == cap(b.rows) {
		return b.flush(ctx)
	}
	return nil
}

func (b *importBatches) Complete(_ context.Context, tick domain.TimestampComplete) error {
	// The registered D2 source has all 18 hosts and channels on every tick.
	// Grid replay has already validated order, canonical host IDs and uniqueness.
	if tick.Records != 18 || tick.ValidRecords != 18 {
		return fmt.Errorf("incomplete registered D2 group %s at %s", tick.GroupID, tick.Timestamp)
	}
	return nil
}

func (b *importBatches) flush(ctx context.Context) error {
	if len(b.rows) == 0 {
		return nil
	}
	result, err := b.writer.WriteBatch(ctx, b.rows)
	if err != nil {
		return err
	}
	b.measurements += int64(result.Measurements)
	b.rows = b.rows[:0]
	return nil
}
