package pgcleanup

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type rollbackBlackhole struct {
	net.Conn
	drop atomic.Bool
	sent chan struct{}
}

func (c *rollbackBlackhole) Write(p []byte) (int, error) {
	if c.drop.CompareAndSwap(true, false) {
		close(c.sent)
		return len(p), nil // Lose just the cleanup request on this test connection.
	}
	return c.Conn.Write(p)
}

func TestRollbackBlackhole(t *testing.T) {
	dsn := os.Getenv("SYSTEM_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	for _, bounded := range []bool{false, true} {
		name := "baseline_unbounded"
		if bounded {
			name = "bounded_and_discarded"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			config, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			config.MaxConns = 1
			connections := make(chan *rollbackBlackhole, 8)
			dial := config.ConnConfig.DialFunc
			config.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dial(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				fault := &rollbackBlackhole{Conn: conn, sent: make(chan struct{})}
				connections <- fault
				return fault, nil
			}
			pool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			first := <-connections
			defer first.Close()
			first.drop.Store(true)
			result := make(chan error, 1)
			go func() {
				if bounded {
					result <- Rollback(tx)
				} else {
					result <- tx.Rollback(context.Background())
				}
			}()
			select {
			case <-first.sent:
			case <-ctx.Done():
				t.Fatal("rollback was not issued")
			}
			if !bounded {
				select {
				case err := <-result:
					t.Fatalf("baseline returned without a response or a deadline: %v", err)
				case <-time.After(250 * time.Millisecond):
				}
				_ = first.Close()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("rollback success fabricated with no server response")
				}
			case <-time.After(4 * time.Second):
				_ = first.Close()
				<-result
				t.Fatal("rollback did not return within budget")
			}
			if err := pool.Ping(ctx); err != nil {
				t.Fatal("pool slot was not released:", err)
			}
			select {
			case <-connections:
			default:
				t.Fatal("the connection with the unresolved cleanup was reused")
			}
		})
	}
}
