package db

import (
	"context"
	"embed"
	"io/fs"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed archive_migrations/*.sql
var archiveControl embed.FS

// Archive databases have their own migration ledger. They start with the
// original project V1 schema and never install the diagnostic migrations.
type archiveSeries struct{}

func (archiveSeries) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "migrations" {
		return nil, fs.ErrNotExist
	}
	baseline, err := Files.ReadDir(name)
	if err != nil {
		return nil, err
	}
	control, err := archiveControl.ReadDir("archive_migrations")
	if err != nil {
		return nil, err
	}
	for _, entry := range baseline {
		if entry.Name() == "V1__core_schema.sql" {
			return append([]fs.DirEntry{entry}, control...), nil
		}
	}
	return nil, fs.ErrNotExist
}

func (archiveSeries) Open(name string) (fs.File, error) {
	if name == "migrations/V1__core_schema.sql" {
		return Files.Open(name)
	}
	if strings.HasPrefix(name, "migrations/") {
		return archiveControl.Open("archive_migrations/" + strings.TrimPrefix(name, "migrations/"))
	}
	return nil, fs.ErrNotExist
}

func ApplyArchive(ctx context.Context, conn *pgx.Conn) (int, error) {
	return Apply(ctx, conn, archiveSeries{})
}
