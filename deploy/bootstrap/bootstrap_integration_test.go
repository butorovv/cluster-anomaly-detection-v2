package main

import (
	"context"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/worker"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNativeBootstrapRestrictedRolesAndRestart(t *testing.T) {
	if os.Getenv("P12_BOOTSTRAP_INTEGRATION") != "1" {
		t.Skip("opt-in real Kafka/PostgreSQL + frozen assets")
	}
	assets, brokers := os.Getenv("SYSTEM_ASSETS_ROOT"), os.Getenv("P07_TEST_KAFKA_BROKERS")
	if assets == "" || brokers == "" || os.Getenv("SYSTEM_KAFKA_ISOLATED") != "1" {
		t.Fatal("explicit isolated prerequisites required")
	}
	pool := postgrestest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	execution, err := worker.NewOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	suffix := strings.ReplaceAll(execution, "-", "")
	workerRole, masterRole := "p12_w_"+suffix, "p12_m_"+suffix
	t.Cleanup(func() {
		for _, role := range []string{workerRole, masterRole} {
			quoted := pgx.Identifier{role}.Sanitize()
			if _, err := pool.Exec(context.Background(), "DROP OWNED BY "+quoted+";DROP ROLE IF EXISTS "+quoted); err != nil {
				t.Error(err)
			}
		}
	})
	for key, value := range map[string]string{"DATABASE_URL": pool.Config().ConnString(), "ASSETS_ROOT": assets, "KAFKA_BROKERS": brokers, "EXECUTION_ID": execution, "WORKER_DB_ROLE": workerRole, "MASTER_DB_ROLE": masterRole, "WORKER_DB_PASSWORD": "worker-" + suffix, "MASTER_DB_PASSWORD": "master-" + suffix, "GROUPS_COUNT": "16", "DELTA_SECONDS": "30", "CHECKPOINT_INTERVAL_TICKS": "3", "OUTPUT_TRANSPORT": "kafka", "PROFILE_SHA256": "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36", "INPUT_SHA256": "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7"} {
		t.Setenv(key, value)
	}
	if err := run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx); err != nil {
		t.Fatal("idempotent bootstrap", err)
	}
	var executions, offsets, cadence int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.executions),(SELECT count(*) FROM cad_v2.result_consumer_offsets),checkpoint_interval_ticks FROM cad_v2.executions`).Scan(&executions, &offsets, &cadence); err != nil || executions != 1 || offsets != 16 || cadence != 3 {
		t.Fatal("bootstrap registration mismatch", executions, offsets, cadence, err)
	}
	for _, role := range []string{workerRole, masterRole} {
		cfg := pool.Config().ConnConfig.Copy()
		cfg.User = role
		cfg.Password = "worker-" + suffix
		if role == masterRole {
			cfg.Password = "master-" + suffix
		}
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer c.Close(context.Background())
			var who string
			if err := c.QueryRow(ctx, "SELECT current_user").Scan(&who); err != nil || who != role {
				t.Fatal("runtime role not active", who, err)
			}
			if _, err := c.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions FOR UPDATE`); err == nil {
				t.Fatal("runtime can update immutable execution metadata")
			}
			if role == masterRole {
				if _, err := c.Exec(ctx, `SELECT 1 FROM cad_v2.outbox_messages LIMIT 0`); err == nil {
					t.Fatal("Master can SQL-poll Worker outbox")
				}
				if _, err := c.Exec(ctx, `SELECT 1 FROM cad_v2.master_results LIMIT 0`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := c.Exec(ctx, `SELECT 1 FROM cad_v2.group_checkpoints LIMIT 0`); err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	t.Log(fmt.Sprintf("fresh + repeat bootstrap, cadence=%d, output cursors=%d, restricted LOGIN roles verified", cadence, offsets))
}
