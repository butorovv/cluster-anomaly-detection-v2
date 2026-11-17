package resulttransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func TestSyncUnresolvedPublishClosesAndJoinsSender(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	closed, exited := make(chan struct{}), make(chan struct{})
	err := awaitSyncPublish(ctx, func(context.Context) error {
		defer close(exited)
		<-closed
		return nil // A successful callback racing with close is too late.
	}, func() error { close(closed); return nil })
	if !domain.IsPermanentDelivery(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unresolved publish must stop this sender: %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("unjoined publish")
	}
}
