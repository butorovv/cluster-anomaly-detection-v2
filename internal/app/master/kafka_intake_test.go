package master

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
)

func TestKafkaIntakeModeCLI(t *testing.T) {
	var help bytes.Buffer
	err := RunCLI(context.Background(), []string{"--help"}, &help)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "-kafka-intake-mode") ||
		!strings.Contains(help.String(), `(default "wave")`) {
		t.Fatalf("Kafka intake flag missing from help: %v\n%s", err, &help)
	}

	t.Setenv("MASTER_DATABASE_URL", "")
	for _, mode := range []string{"wave", "pipeline"} {
		err := RunCLI(context.Background(), []string{"--mode=kafka", "--kafka-intake-mode=" + mode}, &bytes.Buffer{})
		if err == nil || err.Error() != "MASTER_DATABASE_URL required" {
			t.Fatalf("mode %q did not pass flag validation: %v", mode, err)
		}
	}
	err = RunCLI(context.Background(), []string{"--mode=kafka", "--kafka-intake-mode=unknown"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "invalid Kafka intake mode") {
		t.Fatalf("invalid mode accepted: %v", err)
	}
}
