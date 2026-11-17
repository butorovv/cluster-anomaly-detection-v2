package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func p16CheckShutdown(worker, sentSIGTERM bool, exitCode int, log io.Reader) error {
	if !sentSIGTERM {
		return fmt.Errorf("process exited without a successful harness SIGTERM")
	}
	canceled := worker && exitCode == 130
	if exitCode != 0 && !canceled {
		return fmt.Errorf("unexpected exit code %d", exitCode)
	}

	scanner := bufio.NewScanner(log)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	terminal := false
	for line := 1; scanner.Scan(); line++ {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		if terminal {
			return fmt.Errorf("log output after Worker shutdown receipt at line %d", line)
		}
		var entry struct {
			Level *slog.Level `json:"level"`
			Msg   string      `json:"msg"`
			Error string      `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil || entry.Level == nil || entry.Msg == "" {
			return fmt.Errorf("unexpected child log output at line %d", line)
		}
		if *entry.Level < slog.LevelError {
			continue
		}
		if !canceled || *entry.Level != slog.LevelError || entry.Msg != "Worker stopped" {
			return fmt.Errorf("child logged an error at line %d (%s)", line, entry.Msg)
		}
		// An empty topic and partition -1 identify the canceled poll itself.
		// errors.Join can render multiple causes separated by newlines.
		for _, cause := range strings.Split(entry.Error, "\n") {
			switch cause {
			case "context canceled", "Kafka fetch /-1: context canceled":
			default:
				return fmt.Errorf("Worker shutdown includes an unexpected cause at line %d", line)
			}
		}
		terminal = true
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read child log: %w", err)
	}
	if canceled && !terminal {
		return fmt.Errorf("Worker exit 130 without a cancellation receipt")
	}
	return nil
}

func TestP16ShutdownClassification(t *testing.T) {
	const info = `{"level":"INFO","msg":"partition revoked/lost","partition":12}` + "\n"
	receipt := func(cause string) string {
		return `{"level":"ERROR","msg":"Worker stopped","error":` + fmt.Sprintf("%q", cause) + "}\n"
	}
	wrapped := receipt("Kafka fetch /-1: context canceled")
	const kafkaFault = `{"level":"ERROR","msg":"Kafka produce failed","error":"broker unavailable"}` + "\n"
	const sqlFault = `{"level":"ERROR","msg":"SQL rollback failed","error":"connection lost"}` + "\n"
	tests := []struct {
		name        string
		worker      bool
		sentSIGTERM bool
		code        int
		log         string
		wantError   bool
	}{
		{"clean Master", false, true, 0, info, false},
		{"clean Worker", true, true, 0, info, false},
		{"bare cancellation", true, true, 130, info + receipt("context canceled"), false},
		{"Kafka poll cancellation", true, true, 130, info + wrapped, false},
		{"joined cancellations", true, true, 130, receipt("context canceled\nKafka fetch /-1: context canceled"), false},
		{"premature Worker cancellation", true, false, 130, wrapped, true},
		{"premature clean exit", true, false, 0, info, true},
		{"Master cancellation", false, true, 130, wrapped, true},
		{"unexpected exit code", true, true, 1, wrapped, true},
		{"killed after timeout", true, true, -1, wrapped, true},
		{"missing receipt", true, true, 130, info, true},
		{"empty cause", true, true, 130, receipt(""), true},
		{"panic", true, true, 2, "panic: unexpected shutdown\n", true},
		{"panic output before receipt", true, true, 130, "panic: unexpected shutdown\n" + wrapped, true},
		{"Kafka error before receipt", true, true, 130, kafkaFault + wrapped, true},
		{"SQL error before receipt", true, true, 130, sqlFault + wrapped, true},
		{"SQL error on clean exit", false, true, 0, sqlFault, true},
		{"joined SQL failure", true, true, 130, receipt("context canceled\nSQL rollback: connection lost"), true},
		{"Kafka timeout", true, true, 130, receipt("Kafka fetch /-1: context deadline exceeded"), true},
		{"Kafka partition error", true, true, 130, receipt("Kafka fetch telemetry.raw/0: context canceled"), true},
		{"output after receipt", true, true, 130, wrapped + info, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p16CheckShutdown(tt.worker, tt.sentSIGTERM, tt.code, strings.NewReader(tt.log))
			if (err != nil) != tt.wantError {
				t.Fatalf("error = %v, want error: %t", err, tt.wantError)
			}
		})
	}
}
