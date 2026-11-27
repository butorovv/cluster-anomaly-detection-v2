package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
)

const sourceSHA = "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7"

func run() error {
	assets := flag.String("assets", "", "operator-configured Summit assets root")
	host := flag.String("node", "", "Summit hostname")
	first := flag.String("from", "", "inclusive UTC start")
	last := flag.String("to", "", "inclusive UTC end")
	flag.Parse()
	if flag.NArg() != 0 || !filepath.IsAbs(*assets) {
		return fmt.Errorf("expected an absolute assets root")
	}
	if _, _, err := topology.ParseHostname(*host); err != nil {
		return err
	}
	from, err := time.Parse(time.RFC3339, *first)
	if err != nil {
		return err
	}
	to, err := time.Parse(time.RFC3339, *last)
	if err != nil {
		return err
	}
	earliest := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	if from.Before(earliest) || !to.Before(earliest.Add(8*time.Hour)) ||
		to.Before(from) || to.Sub(from) > 10*time.Minute {
		return fmt.Errorf("window outside the registered Summit D2 period or exceeds 10 minutes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	input := filepath.Join(*assets, "data", "prepared", "summit-D1-pilot", "D1.parquet")
	reader, _, err := parquet.OpenVerifiedParquet(ctx, input, sourceSHA)
	if err != nil {
		return err
	}
	samples, err := reader.PhysicalWindow(ctx, *host, from, to)
	if err = errors.Join(err, reader.Close()); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(samples)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
