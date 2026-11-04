// Administrative Compose initialization; this is not a fifth application runtime.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/worker"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	bounded, done := context.WithTimeout(ctx, 3*time.Minute)
	defer done()
	if err := run(bounded); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	execution, assets, dsn := os.Getenv("EXECUTION_ID"), os.Getenv("ASSETS_ROOT"), os.Getenv("DATABASE_URL")
	if execution == "" || assets == "" || dsn == "" {
		return fmt.Errorf("EXECUTION_ID,ASSETS_ROOT,DATABASE_URL required")
	}
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("bootstrap database unavailable")
	}
	defer c.Close(context.Background())
	if _, err := db.Apply(ctx, c, db.Files); err != nil {
		return err
	}
	workerRole, masterRole := env("WORKER_DB_ROLE", "cad_worker"), env("MASTER_DB_ROLE", "cad_master")
	if workerRole == masterRole {
		return fmt.Errorf("distinct runtime roles required")
	}
	for _, role := range []struct{ name, key string }{{workerRole, "WORKER_DB_PASSWORD"}, {masterRole, "MASTER_DB_PASSWORD"}} {
		password := os.Getenv(role.key)
		if len(password) < 16 {
			return fmt.Errorf("%s must have at least 16 characters", role.key)
		}
		var exists bool
		if err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.name).Scan(&exists); err != nil {
			return err
		}
		quoted := pgx.Identifier{role.name}.Sanitize()
		if !exists {
			if _, err := c.Exec(ctx, "CREATE ROLE "+quoted+" NOINHERIT LOGIN"); err != nil {
				return fmt.Errorf("runtime role creation failed")
			}
		}
		var privileged bool
		if err := c.QueryRow(ctx, `SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) OR EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname='cad_v2' AND n.nspowner=r.oid) FROM pg_roles r WHERE rolname=$1`, role.name).Scan(&privileged); err != nil || privileged {
			return fmt.Errorf("bootstrap refuses privileged runtime role")
		}
		if _, err := c.Exec(ctx, "ALTER ROLE "+quoted+" LOGIN PASSWORD '"+strings.ReplaceAll(password, "'", "''")+"'"); err != nil {
			return fmt.Errorf("runtime credential configuration failed")
		}
	}
	if err := db.GrantRuntime(ctx, c, workerRole, masterRole); err != nil {
		return err
	}
	seeds := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	policy := "delete"
	created, err := admin.CreateTopics(ctx, 16, 1, map[string]*string{"cleanup.policy": &policy}, kafkainput.Topic)
	if err != nil {
		return err
	}
	if err := created[kafkainput.Topic].Err; err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	if _, err := kafkainput.CheckTopic(ctx, client, kafkainput.Topic); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	var registered bool
	if err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cad_v2.executions WHERE execution_id=$1::uuid)`, execution).Scan(&registered); err != nil {
		return err
	}
	offsets := map[string]int64{}
	if registered {
		offsets, err = checkpoints.ReadStartOffsets(ctx, pool, execution)
		if err != nil {
			return err
		}
	} else {
		ends, err := admin.ListEndOffsets(ctx, kafkainput.Topic)
		if err != nil {
			return err
		}
		for p := int32(0); p < 16; p++ {
			v, ok := ends[kafkainput.Topic][p]
			if !ok || v.Err != nil {
				return fmt.Errorf("input initial end unavailable")
			}
			offsets[strconv.Itoa(int(p))] = v.Offset
		}
	}
	encoded, err := json.Marshal(offsets)
	if err != nil {
		return err
	}
	if err := os.Setenv("WORKER_DATABASE_URL", dsn); err != nil {
		return err
	}
	mode := env("OUTPUT_TRANSPORT", "http_push")
	args := []string{"--register-execution", "--assets", assets, "--profile-sha256", os.Getenv("PROFILE_SHA256"), "--input-sha256", os.Getenv("INPUT_SHA256"), "--execution-id", execution, "--consumer-group", "cad-input-" + execution, "--kafka-brokers", strings.Join(seeds, ","), "--start-offsets", string(encoded), "--groups", env("GROUPS_COUNT", "16"), "--delta", env("DELTA_SECONDS", "0"), "--checkpoint-interval-ticks", env("CHECKPOINT_INTERVAL_TICKS", "1"), "--output-transport", mode}
	if err := worker.RunCLI(ctx, args, os.Stderr); err != nil {
		return err
	}
	if mode == "kafka" {
		sender, err := resulttransport.NewKafka(ctx, seeds, true)
		if err != nil {
			return err
		}
		pin := sender.Identity
		_ = sender.Close()
		ends, err := admin.ListEndOffsets(ctx, resulttransport.ResultsTopic)
		if err != nil {
			return err
		}
		starts := map[string]int64{}
		for p := int32(0); p < 16; p++ {
			v, ok := ends[resulttransport.ResultsTopic][p]
			if !ok || v.Err != nil {
				return fmt.Errorf("output initial end unavailable")
			}
			starts[strconv.Itoa(int(p))] = v.Offset
		}
		if err := (events.MasterStore{DB: pool}).BindResults(ctx, execution, pin.ClusterID, pin.TopicID, "cad-results-"+execution, starts); err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stdout, "bootstrap complete; runtime roles and immutable execution registered")
	return nil
}
