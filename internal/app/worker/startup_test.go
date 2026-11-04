package worker

import (
	"context"
	"sync/atomic"
	"testing"

	delivery "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
)

type startupSender struct{ closed atomic.Int32 }

func (*startupSender) Send(context.Context, []byte, string) error { return nil }
func (s *startupSender) Close() error                             { s.closed.Add(1); return nil }

func TestSourceFailureClosesOutputBeforeDispatcherStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sender := &startupSender{}
	dispatcher := &delivery.Dispatcher{Sender: sender}
	runner := &Runner{cfg: Config{Broker: kafkainput.Broker{Seeds: []string{"broker.invalid:9092"}}, Output: dispatcher}}
	if err := runner.Run(ctx); err == nil {
		t.Fatal("source failure unexpectedly succeeded")
	}
	if sender.closed.Load() != 1 {
		t.Fatal("sender leaked before dispatcher startup", sender.closed.Load())
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatal(err)
	}
	if sender.closed.Load() != 1 {
		t.Fatal("sender closed more than once")
	}
}
