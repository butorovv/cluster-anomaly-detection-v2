// Package postgres binds Worker state/progress/output to the existing V1-V5 SQL API.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type Registration struct {
	Identity                          group.RBI2Identity
	Binding                           telemetry.InputBinding
	ClusterID, TopicID, ConsumerGroup string
	StartOffsets                      map[string]int64
	OutputTransport                   string
	Diagnostics                       bool // immutable v2 transport contract, separate from scientific identity
}

type sourceMetadata struct {
	Version     int                    `json:"version"`
	TopicID     string                 `json:"topic_id"`
	Binding     telemetry.InputBinding `json:"binding"`
	Diagnostics bool                   `json:"diagnostics,omitempty"`
}

func hashJSON(v any) string { b, _ := json.Marshal(v); return core.SnapshotHash(b) }
func uuidValid(v string) bool {
	var id pgtype.UUID
	return id.Scan(v) == nil && id.Valid && id.String() == v
}

func (r Registration) validate() error {
	if !uuidValid(r.Binding.ExecutionID) || r.Binding.Validate() != nil || r.ClusterID == "" || r.TopicID == "" || r.ConsumerGroup == "" || (r.Identity.Groups != 4 && r.Identity.Groups != 8 && r.Identity.Groups != 16) {
		return fmt.Errorf("explicit UUID execution/source/topic/group and 4/8/16-group registration required")
	}
	id, err := group.NewRBI2Identity(r.Identity.ProfileSHA256, r.Identity.InputSHA256, r.Identity.Groups, r.Identity.Window, r.Identity.DeltaSeconds)
	if err != nil || hashJSON(id) != hashJSON(r.Identity) || r.Binding.ProfileSHA256 != id.ProfileSHA256 {
		return fmt.Errorf("invalid frozen run registration")
	}
	if len(r.StartOffsets) != 16 {
		return fmt.Errorf("16 explicit initial offsets required")
	}
	for i := 0; i < 16; i++ {
		n, ok := r.StartOffsets[strconv.Itoa(i)]
		if !ok || n < 0 {
			return fmt.Errorf("invalid initial offset for partition %d", i)
		}
	}
	if r.OutputTransport != "polling" && r.OutputTransport != "http_push" && r.OutputTransport != "kafka" {
		return fmt.Errorf("reserved output_transport metadata required")
	}
	return nil
}

func registrationHashes(r Registration) (string, string, string) {
	return hashJSON([]any{"cad-bundle-v1", r.Identity.ProfileSHA256, r.Identity.InputSHA256}), hashJSON(topology.CanonicalGroups()), hashJSON([]any{"F18-v1", telemetry.FeatureNames})
}

// Register is an explicit administrative operation, never implicit Worker startup.
// _p07 is an immutable metadata member of the existing JSON object; seed_partitions
// continues to read only numeric keys 0..15. No migration or altered math ID.
func Register(ctx context.Context, db DB, r Registration) error {
	if err := r.validate(); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	bundle, topology, features := registrationHashes(r)
	identity, _ := json.Marshal(r.Identity)
	inputs, _ := json.Marshal(r.Identity.InputSHA256)
	_, err = tx.Exec(ctx, `INSERT INTO cad_v2.runs(run_id,config_id,legacy_run_id,kernel_version,profile_sha256,input_sha256,bundle_sha256,topology_sha256,feature_schema_sha256,groups_count,delta_seconds,window_start,window_end,identity_json)
 VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14::jsonb) ON CONFLICT DO NOTHING`, r.Identity.RunID, r.Identity.ConfigID, r.Identity.LegacyRunID, r.Identity.Version, r.Identity.ProfileSHA256, inputs, bundle, topology, features, r.Identity.Groups, r.Identity.DeltaSeconds, r.Identity.Window.Start, r.Identity.Window.End, identity)
	if err != nil {
		return err
	}
	offsets := map[string]any{}
	for k, v := range r.StartOffsets {
		offsets[k] = v
	}
	offsets["_p07"] = sourceMetadata{Version: metadataVersion(r.Diagnostics), TopicID: r.TopicID, Binding: r.Binding, Diagnostics: r.Diagnostics}
	start, _ := json.Marshal(offsets)
	_, err = tx.Exec(ctx, `INSERT INTO cad_v2.executions(execution_id,run_id,kafka_cluster_id,input_topic,consumer_group,partition_count,output_transport,checkpoint_interval_ticks,snapshot_schema_version,snapshot_codec,start_offsets)
 VALUES($1::uuid,$2,$3,'telemetry.raw',$4,16,$5,1,$6,'json-utf8',$7::jsonb) ON CONFLICT DO NOTHING`, r.Binding.ExecutionID, r.Identity.RunID, r.ClusterID, r.ConsumerGroup, r.OutputTransport, group.MathSnapshotSchemaVersion, start)
	if err != nil {
		return err
	}
	if err := validateRegistration(ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type rowQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ValidateRegistration rejects absent/adopted/foreign executions. It also checks
// the run columns used by SQL validation, not just the identity_json declaration.
func ValidateRegistration(ctx context.Context, db DB, r Registration) error {
	if err := r.validate(); err != nil {
		return err
	}
	return validateRegistration(ctx, db, r)
}

// ReadStartOffsets reads registered numeric keys only. Full metadata, including
// topic incarnation, is independently validated before any acquire/processing.
func ReadStartOffsets(ctx context.Context, db DB, execution string) (map[string]int64, error) {
	var raw []byte
	if err := db.QueryRow(ctx, "SELECT start_offsets FROM cad_v2.executions WHERE execution_id=$1::uuid", execution).Scan(&raw); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	result := map[string]int64{}
	for i := 0; i < 16; i++ {
		key := strconv.Itoa(i)
		var n int64
		if err := json.Unmarshal(object[key], &n); err != nil || n < 0 {
			return nil, fmt.Errorf("invalid registered start offsets")
		}
		result[key] = n
	}
	return result, nil
}
func validateRegistration(ctx context.Context, q rowQuery, r Registration) error {
	var got group.RBI2Identity
	var inputs, identity, offsetBytes []byte
	var bundle, topology, features, cluster, topic, consumer, output, codec string
	var partitions, cadence, version int
	err := q.QueryRow(ctx, `SELECT r.run_id,r.config_id,r.legacy_run_id,r.kernel_version,r.profile_sha256,r.input_sha256,r.groups_count,r.delta_seconds,r.window_start,r.window_end,r.identity_json,r.bundle_sha256,r.topology_sha256,r.feature_schema_sha256,x.kafka_cluster_id,x.input_topic,x.consumer_group,x.output_transport,x.partition_count,x.checkpoint_interval_ticks,x.snapshot_schema_version,x.snapshot_codec,x.start_offsets
 FROM cad_v2.executions x JOIN cad_v2.runs r USING(run_id) WHERE x.execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&got.RunID, &got.ConfigID, &got.LegacyRunID, &got.Version, &got.ProfileSHA256, &inputs, &got.Groups, &got.DeltaSeconds, &got.Window.Start, &got.Window.End, &identity, &bundle, &topology, &features, &cluster, &topic, &consumer, &output, &partitions, &cadence, &version, &codec, &offsetBytes)
	if err != nil {
		return fmt.Errorf("registered execution required: %w", err)
	}
	// timestamptz preserves instants, not the session timezone. Frozen run IDs
	// use UTC JSON; do not let the server's display zone change that identity.
	got.Window.Start, got.Window.End = got.Window.Start.UTC(), got.Window.End.UTC()
	if err = json.Unmarshal(inputs, &got.InputSHA256); err != nil {
		return err
	}
	var savedIdentity group.RBI2Identity
	if err = json.Unmarshal(identity, &savedIdentity); err != nil {
		return err
	}
	bh, th, fh := registrationHashes(r)
	if hashJSON(got) != hashJSON(r.Identity) || hashJSON(savedIdentity) != hashJSON(r.Identity) || bundle != bh || topology != th || features != fh || cluster != r.ClusterID || topic != "telemetry.raw" || consumer != r.ConsumerGroup || output != r.OutputTransport || partitions != 16 || cadence != 1 || version != group.MathSnapshotSchemaVersion || codec != "json-utf8" {
		return fmt.Errorf("execution frozen/source/snapshot metadata mismatch")
	}
	var offsets map[string]json.RawMessage
	if err = json.Unmarshal(offsetBytes, &offsets); err != nil {
		return err
	}
	var meta sourceMetadata
	if err = json.Unmarshal(offsets["_p07"], &meta); err != nil {
		return fmt.Errorf("P07 topic binding registration required")
	}
	if meta.Version != metadataVersion(r.Diagnostics) || meta.Diagnostics != r.Diagnostics || meta.TopicID != r.TopicID || meta.Binding != r.Binding || len(offsets) != 17 {
		return fmt.Errorf("execution topic incarnation/input binding mismatch")
	}
	for i := 0; i < 16; i++ {
		key := strconv.Itoa(i)
		var n int64
		if err = json.Unmarshal(offsets[key], &n); err != nil || n != r.StartOffsets[key] {
			return fmt.Errorf("execution start offset mismatch")
		}
	}
	return nil
}

// Timeouts are connection-local. Runtime roles need only SELECT and the SQL
// security-definer functions, as specified by db/scripts/grants.sql.
const DefaultLease = 10 * time.Second

func metadataVersion(diagnostics bool) int {
	if diagnostics {
		return 2
	}
	return 1
}
