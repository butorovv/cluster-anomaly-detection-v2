package postgres_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
)

// Opt-in observation, not a deterministic fault test. Only PostgreSQL itself
// acquires extension locks here. There is no advisory/table-lock substitute and
// no assertion that a brief observed extension wait recreates a production stall.
// At most 4 writers * 8 batches * 512 rows * 4096 bytes (~64 MiB plus overhead).
func TestObserveRelationExtension(t *testing.T) {
	if os.Getenv("P14_OBSERVE_RELATION_EXTENSION") != "1" {
		t.Skip("opt-in bounded real relation-extension observation")
	}
	pool := postgrestest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `CREATE TABLE public.p14_extension_probe(payload text NOT NULL); ALTER TABLE public.p14_extension_probe ALTER COLUMN payload SET STORAGE EXTERNAL`); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for batch := 0; batch < 8; batch++ {
				if _, err := pool.Exec(ctx, `INSERT INTO public.p14_extension_probe SELECT repeat(md5(i::text),128) FROM generate_series(1,512) i`); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()
	close(start)
	var witness json.RawMessage
	samples := 0
observe:
	for {
		var raw []byte
		err := pool.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_object(
 'at',clock_timestamp(),'pid',l.pid,'relation',l.relation::regclass::text,
 'mode',l.mode,'granted',l.granted,'wait_type',a.wait_event_type,'wait_event',a.wait_event,
 'blockers',pg_blocking_pids(l.pid))), '[]'::jsonb)
 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='extend' AND NOT l.granted AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&raw)
		if err != nil {
			t.Fatal(err)
		}
		samples++
		if string(raw) != "[]" && witness == nil {
			witness = append(json.RawMessage(nil), raw...)
		}
		select {
		case <-done:
			break observe
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if witness == nil {
		t.Logf("NOT_OBSERVED: no ungranted real extension lock in %d samples; no fault-recovery claim", samples)
		return
	}
	t.Logf("OBSERVED real extension wait (%d samples): %s; duration and production cause not inferred", samples, witness)
}
