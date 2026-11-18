package pgcleanup

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Rollback gives cleanup a fresh, bounded context after request cancellation.
// pgx discards the connection if rollback cannot be confirmed.
func Rollback(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return tx.Rollback(ctx)
}
