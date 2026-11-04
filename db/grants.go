package db

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
)

//go:embed scripts/grants.sql
var runtimeGrants string

// GrantRuntime applies the same reviewed psql grants using existing runtime roles.
// Provisioning belongs to explicit administrative bootstrap, never Worker startup.
func GrantRuntime(ctx context.Context, c *pgx.Conn, worker, master string) error {
	if worker == "" || master == "" || worker == master {
		return fmt.Errorf("distinct existing runtime roles required")
	}
	sql := strings.ReplaceAll(runtimeGrants, `:"worker_role"`, pgx.Identifier{worker}.Sanitize())
	sql = strings.ReplaceAll(sql, `:"master_role"`, pgx.Identifier{master}.Sanitize())
	_, err := c.Exec(ctx, sql)
	return err
}
