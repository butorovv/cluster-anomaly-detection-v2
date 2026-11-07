package master

import (
	"context"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
)

func RunKafka(ctx context.Context, seeds []string, consumer, execution string, create bool, store repo.MasterStore, master usecase.Master, options ...resulttransport.KafkaConsumerConfig) error {
	if len(options) > 1 {
		return fmt.Errorf("one Kafka intake configuration required")
	}
	cfg := resulttransport.KafkaConsumerConfig{}.WithDefaults()
	if len(options) == 1 {
		cfg = options[0].WithDefaults()
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	sender, err := resulttransport.NewKafka(ctx, seeds, create, master.Metrics)
	if err != nil {
		return err
	}
	pin := sender.Identity
	_ = sender.Close()
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer adminClient.Close()
	starts, err := kadm.NewClient(adminClient).ListStartOffsets(ctx, resulttransport.ResultsTopic)
	if err != nil {
		return err
	}
	offsets := map[string]int64{}
	for p := int32(0); p < 16; p++ {
		position, ok := starts[resulttransport.ResultsTopic][p]
		if !ok || position.Err != nil {
			return fmt.Errorf("results partition start unavailable")
		}
		offsets[strconv.Itoa(int(p))] = position.Offset
	}
	if err := store.BindResults(ctx, execution, pin.ClusterID, pin.TopicID, consumer, offsets); err != nil {
		return err
	}
	return resulttransport.ConsumeKafkaWithConfig(ctx, seeds, consumer, pin, func(ctx context.Context, p int32) (int64, error) { return store.ResultOffset(ctx, execution, p) }, func(ctx context.Context, r *kgo.Record, b []byte) error {
		return master.AcceptKafka(ctx, execution, pin.TopicID, r.Partition, r.Offset, b)
	}, cfg, master.Metrics)
}
