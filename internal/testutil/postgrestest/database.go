// Package postgrestest creates only uniquely named disposable test databases.
package postgrestest

import (
	"context"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv("SYSTEM_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("SYSTEM_TEST_DATABASE_URL required for real PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("system_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(base)
	if err != nil || !strings.HasPrefix(u.Scheme, "postgres") {
		t.Fatal("PostgreSQL URL required")
	}
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		_ = admin.Close(cleanup)
	})
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Apply(ctx, connection.Conn(), db.Files)
	connection.Release()
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
