// Package db owns the single ordered V<number>__description.sql migration series.
package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var Files embed.FS

var migrationName = regexp.MustCompile(`^V([1-9][0-9]*)__[a-z0-9_]+\.sql$`)

type migration struct {
	version        int
	name, sql, sha string
}

func readSeries(files fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(files, "migrations")
	if err != nil {
		return nil, err
	}
	var series []migration
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("unexpected migration directory: %s", entry.Name())
		}
		m := migrationName.FindStringSubmatch(entry.Name())
		if m == nil {
			return nil, fmt.Errorf("invalid migration name: %s", entry.Name())
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, err
		}
		b, err := fs.ReadFile(files, "migrations/"+entry.Name())
		if err != nil {
			return nil, err
		}
		h := sha256.Sum256(b)
		series = append(series, migration{version, entry.Name(), string(b), hex.EncodeToString(h[:])})
	}
	sort.Slice(series, func(i, j int) bool { return series[i].version < series[j].version })
	if len(series) == 0 {
		return nil, fmt.Errorf("empty migration series")
	}
	for i, m := range series {
		if m.version != i+1 {
			return nil, fmt.Errorf("duplicate or non-contiguous migration version: %d", m.version)
		}
	}
	return series, nil
}

// Apply transactionally applies pending migrations. It never adopts an existing
// project schema, rewrites checksums, drops data, or rolls migrations down.
// One transaction-scoped advisory lock serializes cooperating runners.
func Apply(ctx context.Context, conn *pgx.Conn, files fs.FS) (applied int, err error) {
	series, err := readSeries(files)
	if err != nil {
		return 0, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(726249201, 1)"); err != nil {
		return 0, err
	}
	var originalPath string
	if err = tx.QueryRow(ctx, "SHOW search_path").Scan(&originalPath); err != nil {
		return 0, err
	}
	var tracked, existing bool
	if err = tx.QueryRow(ctx, "SELECT to_regclass('public.schema_migrations') IS NOT NULL, to_regnamespace('project') IS NOT NULL").Scan(&tracked, &existing); err != nil {
		return 0, err
	}
	if !tracked && existing {
		return 0, fmt.Errorf("untracked project schema: controlled migration adoption required")
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations (
 version integer PRIMARY KEY CHECK(version > 0), name text NOT NULL UNIQUE,
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, "SELECT version,name,sha256 FROM public.schema_migrations ORDER BY version")
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		var version int
		var name, sha string
		if err = rows.Scan(&version, &name, &sha); err != nil {
			rows.Close()
			return 0, err
		}
		if count >= len(series) || version != series[count].version || name != series[count].name || sha != series[count].sha {
			rows.Close()
			return 0, fmt.Errorf("migration history differs at version %d; no automatic repair", version)
		}
		count++
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, m := range series[count:] {
		if _, err = tx.Exec(ctx, m.sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			return 0, fmt.Errorf("%s: %w", m.name, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO public.schema_migrations(version,name,sha256) VALUES($1,$2,$3)", m.version, m.name, m.sha); err != nil {
			return 0, err
		}
		applied++
	}
	// V1 contains SET search_path. Do not leak it to the caller's connection.
	if _, err = tx.Exec(ctx, "SELECT set_config('search_path',$1,false)", originalPath); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return applied, nil
}
