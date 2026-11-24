package telemetryarchive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type StreamConfig struct {
	Binding domain.InputBinding
	Window  domain.InputWindow
	Broker  kafkainput.Broker
	// Summit GUI provisions a fresh broker/topic, so all starts are zero.
	// Other deployments must explicitly supply registered starting offsets.
	Starts [kafkainput.Partitions]int64
}

func InitializeSource(ctx context.Context, pool *pgxpool.Pool, sha string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err = db.ApplyArchive(ctx, conn.Conn()); err != nil {
		return err
	}
	return registerSource(ctx, conn.Conn(), sha)
}

type streamPartition struct {
	epoch, offset int64
	next          time.Time
	assembler     *usecase.TickAssembler
}

type streamStore struct {
	pool           *pgxpool.Pool
	writer         *archive.Writer
	config         StreamConfig
	cluster, topic string
}

func (s streamStore) register(ctx context.Context) error {
	cfg := s.config
	result, err := s.pool.Exec(ctx, `INSERT INTO cad_archive.stream_executions AS e
 (execution_id,source_sha256,profile_sha256,cluster_id,topic_id,window_start,window_end)
 VALUES($1,$2,$3,$4,$5,$6,$7)
 ON CONFLICT(execution_id) DO UPDATE SET execution_id=e.execution_id
 WHERE (e.source_sha256,e.profile_sha256,e.cluster_id,e.topic_id,e.window_start,e.window_end)=
 (excluded.source_sha256,excluded.profile_sha256,excluded.cluster_id,excluded.topic_id,excluded.window_start,excluded.window_end)`,
		cfg.Binding.ExecutionID, cfg.Binding.InputSHA256, cfg.Binding.ProfileSHA256, s.cluster, s.topic, cfg.Window.Start, cfg.Window.End)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("archive execution has another Kafka or source identity")
	}
	return nil
}

func (s streamStore) acquire(ctx context.Context, id kafkainput.Identity) (*streamPartition, error) {
	cfg := s.config
	if id.Binding != cfg.Binding || id.Window != cfg.Window || id.TopicID != s.topic {
		return nil, fmt.Errorf("archive owner identity differs")
	}
	p := &streamPartition{}
	err := s.pool.QueryRow(ctx, `INSERT INTO cad_archive.positions AS p
 (execution_id,partition_id,owner_epoch,next_offset,next_tick) VALUES($1,$2,1,$3,$4)
 ON CONFLICT (execution_id,partition_id) DO UPDATE SET owner_epoch=p.owner_epoch+1
 RETURNING owner_epoch,next_offset,next_tick`, cfg.Binding.ExecutionID, id.Partition, cfg.Starts[id.Partition], cfg.Window.Start).
		Scan(&p.epoch, &p.offset, &p.next)
	if err != nil {
		return nil, err
	}
	if p.next.Before(cfg.Window.End) {
		p.assembler, err = usecase.NewTickAssembler(id.GroupID, cfg.Binding, domain.InputWindow{Start: p.next.UTC(), End: cfg.Window.End})
	}
	return p, err
}

func (s streamStore) save(ctx context.Context, partition int32, owner streamPartition, last int64, tick *domain.GroupTick) (archive.Result, error) {
	if !owner.next.Equal(tick.Timestamp) || last-owner.offset != int64(len(tick.Records)) {
		return archive.Result{}, fmt.Errorf("archive tick does not match durable position")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return archive.Result{}, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	cas, err := tx.Exec(ctx, `UPDATE cad_archive.positions SET next_offset=$6, next_tick=$7
 WHERE execution_id=$1::uuid AND partition_id=$2 AND owner_epoch=$3 AND next_offset=$4 AND next_tick=$5`,
		s.config.Binding.ExecutionID, partition, owner.epoch, owner.offset, owner.next, last+1, tick.Timestamp.Add(domain.GridStep))
	if err != nil {
		return archive.Result{}, err
	}
	if cas.RowsAffected() != 1 {
		return archive.Result{}, fmt.Errorf("stale archive owner or offset")
	}
	masks := [18]int32{}
	for i := range masks {
		masks[i] = -1
	}
	records := make([]domain.TelemetryRecord, 0, len(tick.Records))
	for _, record := range tick.Records {
		masks[record.NodeIndex-1] = int32(record.AvailabilityMask)
		records = append(records, record)
	}
	result, err := s.writer.WriteBatchInTx(ctx, records, tx)
	if err != nil {
		return archive.Result{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO cad_archive.coverage(execution_id,partition_id,tick,masks,inserted,duplicates)
 VALUES($1,$2,$3,$4,$5,$6)`, s.config.Binding.ExecutionID, partition, tick.Timestamp, masks[:], result.Inserted, result.Duplicates)
	if err != nil {
		return archive.Result{}, err
	}
	// On any uncertain COMMIT, leave the in-memory position unchanged and
	// restart via acquire; PostgreSQL determines whether this tick committed.
	if err = tx.Commit(ctx); err != nil {
		return archive.Result{}, err
	}
	return result, nil
}

func Consume(ctx context.Context, pool *pgxpool.Pool, config StreamConfig) error {
	if !events.ValidExecution(config.Binding.ExecutionID) || config.Binding.Validate() != nil || config.Window.Validate() != nil {
		return fmt.Errorf("invalid archive execution")
	}
	timed, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := InitializeSource(timed, pool, config.Binding.InputSHA256); err != nil {
		cancel()
		return err
	}
	writer, err := archive.New(timed, pool, 18)
	if err != nil {
		cancel()
		return err
	}
	cluster, topic, err := kafkainput.DescribeSource(timed, config.Broker)
	cancel()
	if err != nil {
		return err
	}
	store := streamStore{pool: pool, writer: writer, config: config, cluster: cluster, topic: topic}
	if err := store.register(ctx); err != nil {
		return err
	}
	partitions := make(map[int32]*streamPartition)
	var ownership sync.Mutex
	forget := func(ctx context.Context, ids []kafkainput.Identity) error {
		ownership.Lock()
		defer ownership.Unlock()
		for _, id := range ids {
			p := partitions[id.Partition]
			delete(partitions, id.Partition)
			if p == nil {
				continue
			}
			if _, err := pool.Exec(ctx, `UPDATE cad_archive.positions SET owner_epoch=owner_epoch+1
 WHERE execution_id=$1 AND partition_id=$2 AND owner_epoch=$3`,
				config.Binding.ExecutionID, id.Partition, p.epoch); err != nil {
				return err
			}
		}
		return nil
	}
	settings := kafkainput.ConsumerConfig{Broker: config.Broker, Binding: config.Binding, Window: config.Window,
		Group: "cad-archive-" + config.Binding.ExecutionID, Revoked: forget, Lost: forget}
	settings.Load = func(ctx context.Context, id kafkainput.Identity) (kafkainput.Position, error) {
		p, err := store.acquire(ctx, id)
		if err != nil {
			return kafkainput.Position{}, err
		}
		ownership.Lock()
		partitions[id.Partition] = p
		ownership.Unlock()
		return kafkainput.Position{Identity: id, NextOffset: p.offset}, nil
	}
	var ticks, measurements, duplicates int64
	started := time.Now()
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-timer.C:
				var done int
				probe, cancel := context.WithTimeout(streamCtx, time.Second)
				err := pool.QueryRow(probe, `SELECT count(*) FROM cad_archive.positions
 WHERE execution_id=$1 AND next_tick=$2`, config.Binding.ExecutionID, config.Window.End).Scan(&done)
				cancel()
				if err == nil && done == kafkainput.Partitions {
					stop()
					return
				}
			}
		}
	}()
	err = kafkainput.ConsumeGroup(streamCtx, settings, func(ctx context.Context, row *kgo.Record, message domain.InputMessage) error {
		ownership.Lock()
		p := partitions[row.Partition]
		ownership.Unlock()
		if p == nil || p.assembler == nil {
			return fmt.Errorf("received data outside archive ownership or time window")
		}
		tick, err := p.assembler.Accept(message)
		if err != nil || tick == nil {
			return err
		}
		write, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := store.save(write, row.Partition, *p, row.Offset, tick)
		cancel()
		if err != nil {
			return err
		}
		p.offset, p.next = row.Offset+1, tick.Timestamp.Add(domain.GridStep)
		ticks++
		measurements += int64(result.Inserted)
		duplicates += int64(result.Duplicates)
		if ticks%16 == 0 {
			slog.Info("archive durable progress", "ticks", ticks, "inserted", measurements,
				"duplicates", duplicates, "elapsed", time.Since(started))
		}
		return nil
	})
	if errors.Is(err, context.Canceled) && streamCtx.Err() != nil {
		return nil
	}
	return err
}
