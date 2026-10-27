package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
)

func TestBootstrapInventoryAndHistoricalKeys(t *testing.T) {
	p, conn := archiveDB(t)
	ctx := context.Background()
	if err := Bootstrap(ctx, p); err != nil {
		t.Fatal(err)
	}
	var groups, hosts, components, sources int
	qs := []string{
		"SELECT count(*) FROM project.node_group WHERE code='SUMMIT' OR parent_id=(SELECT id FROM project.node_group WHERE code='SUMMIT')",
		"SELECT count(*) FROM project.node", "SELECT count(*) FROM project.component", "SELECT count(*) FROM project.metric_source",
	}
	ds := []*int{&groups, &hosts, &components, &sources}
	for i, q := range qs {
		if err := conn.QueryRow(ctx, q).Scan(ds[i]); err != nil {
			t.Fatal(err)
		}
	}
	if groups != 17 || hosts != 288 || components != 2880 || sources != 5184 {
		t.Fatalf("inventory groups/hosts/components/sources=%d/%d/%d/%d", groups, hosts, components, sources)
	}
	var before int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&before); err != nil || before != 5 {
		t.Fatalf("history=%d %v", before, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE project.metric_source s SET source_key='legacy-spelling' FROM project.component c JOIN project.node n ON n.id=c.node_id WHERE s.component_id=c.id AND n.hostname='a01n01' AND c.kind='psu' AND c.local_index=0 AND s.metric_type_id=(SELECT id FROM project.metric_type WHERE code='PSU_INPUT_POWER')`); err != nil {
		t.Fatal(err)
	}
	var sourceID int32
	if err := conn.QueryRow(ctx, `SELECT s.id FROM project.metric_source s JOIN project.component c ON c.id=s.component_id JOIN project.node n ON n.id=c.node_id JOIN project.metric_type m ON m.id=s.metric_type_id WHERE n.hostname='a01n01' AND c.kind='psu' AND c.local_index=0 AND m.code='PSU_INPUT_POWER'`).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(ctx, p); err != nil {
		t.Fatal(err)
	}
	w, err := New(ctx, p, 1)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := w.SourceIDs("a01n01")
	if err != nil {
		t.Fatal(err)
	}
	if ids[0] != sourceID {
		t.Fatalf("legacy source_key did not map by component and metric: %d != %d", ids[0], sourceID)
	}
	if _, err = w.SourceIDs("unknown"); !errors.Is(err, ErrUnknownSource) {
		t.Fatalf("unknown host: %v", err)
	}
	var key string
	if err = conn.QueryRow(ctx, "SELECT source_key FROM project.metric_source WHERE id=$1", sourceID).Scan(&key); err != nil || key != "legacy-spelling" {
		t.Fatalf("historical key changed to %q: %v", key, err)
	}
	var after int
	if err = conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&after); err != nil || after != before {
		t.Fatalf("migration history changed: %d %v", after, err)
	}
}

func TestSourceIDsMatchIndependentSQLMapping(t *testing.T) {
	p, conn := archiveDB(t)
	ctx := context.Background()
	if err := Bootstrap(ctx, p); err != nil {
		t.Fatal(err)
	}
	w, err := New(ctx, p, 1)
	if err != nil {
		t.Fatal(err)
	}
	type expected struct {
		kind   string
		index  int
		metric string
	}
	want := []expected{
		{"psu", 0, "PSU_INPUT_POWER"}, {"psu", 1, "PSU_INPUT_POWER"},
		{"cpu", 0, "CPU_POWER"}, {"cpu", 1, "CPU_POWER"},
		{"gpu", 0, "GPU_POWER"}, {"gpu", 1, "GPU_POWER"}, {"gpu", 2, "GPU_POWER"}, {"gpu", 3, "GPU_POWER"}, {"gpu", 4, "GPU_POWER"}, {"gpu", 5, "GPU_POWER"},
		{"cpu", 0, "CPU_MEAN_TEMP"}, {"cpu", 1, "CPU_MEAN_TEMP"},
		{"gpu", 0, "GPU_CORE_TEMP"}, {"gpu", 1, "GPU_CORE_TEMP"}, {"gpu", 2, "GPU_CORE_TEMP"}, {"gpu", 3, "GPU_CORE_TEMP"}, {"gpu", 4, "GPU_CORE_TEMP"}, {"gpu", 5, "GPU_CORE_TEMP"},
	}
	ids, err := w.SourceIDs("a01n01")
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range want {
		var id int32
		err := conn.QueryRow(ctx, `SELECT s.id FROM project.metric_source s JOIN project.component c ON c.id=s.component_id JOIN project.node n ON n.id=c.node_id JOIN project.metric_type m ON m.id=s.metric_type_id WHERE n.hostname='a01n01' AND c.kind=$1 AND c.local_index=$2 AND m.code=$3`, e.kind, e.index, e.metric).Scan(&id)
		if err != nil || id != ids[i] {
			t.Errorf("feature %s index=%d metric=%s: mapped=%d SQL=%d err=%v", domain.FeatureNames[i], e.index, e.metric, ids[i], id, err)
		}
	}
}

func TestConcurrentBootstrapAndWriters(t *testing.T) {
	p, _ := archiveDB(t)
	ctx := context.Background()
	const workers = 8
	cfg := p.Config().Copy()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := pgx.ConnectConfig(ctx, cfg.Copy())
			if e == nil {
				defer c.Close(context.Background())
				e = Bootstrap(ctx, c)
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r := recordAt(time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC), "a01n01", domain.FullAvailabilityMask, 42)
	errs = make(chan error, workers)
	ready, start := make(chan struct{}, workers), make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := pgx.ConnectConfig(ctx, cfg.Copy())
			if e == nil {
				defer c.Close(context.Background())
				local, x := New(ctx, c, 1)
				e = x
				ready <- struct{}{}
				<-start
				if e == nil {
					_, e = local.WriteBatch(ctx, []domain.TelemetryRecord{r})
				}
			} else {
				ready <- struct{}{}
			}
			errs <- e
		}()
	}
	for i := 0; i < workers; i++ {
		<-ready
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := p.QueryRow(ctx, "SELECT count(*) FROM project.telemetry").Scan(&count); err != nil || count != 18 {
		t.Fatalf("concurrent rows=%d %v", count, err)
	}
	conflict := recordAt(time.Date(2025, 4, 1, 1, 0, 0, 0, time.UTC), "a01n01", domain.FullAvailabilityMask, 77)
	other := conflict
	other.CPUPower[0]++
	errs = make(chan error, 2)
	ready, start = make(chan struct{}, 2), make(chan struct{})
	for _, rec := range []domain.TelemetryRecord{conflict, other} {
		rec := rec
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := pgx.ConnectConfig(ctx, cfg.Copy())
			if e == nil {
				defer c.Close(context.Background())
				local, x := New(ctx, c, 1)
				e = x
				ready <- struct{}{}
				<-start
				if e == nil {
					_, e = local.WriteBatch(ctx, []domain.TelemetryRecord{rec})
				}
			} else {
				ready <- struct{}{}
			}
			errs <- e
		}()
	}
	<-ready
	<-ready
	close(start)
	wg.Wait()
	close(errs)
	success, conflicts := 0, 0
	for e := range errs {
		if e == nil {
			success++
		} else if errors.Is(e, ErrConflict) {
			conflicts++
		} else {
			t.Errorf("unexpected race error: %v", e)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("conflicting race: success=%d conflict=%d", success, conflicts)
	}
	if err := p.QueryRow(ctx, "SELECT count(*) FROM project.telemetry").Scan(&count); err != nil || count != 36 {
		t.Fatalf("rows after conflicting race=%d %v", count, err)
	}
}
