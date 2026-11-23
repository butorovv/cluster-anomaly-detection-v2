package experiment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunnerRejectsLateExitAfterGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, release := startLifecycleChild(t, ctx, "worker")
	report := Result{Comparison: Comparison{Status: "PASS"}}
	if err := p.failure(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.await(ctx); err != nil {
		t.Fatal(err)
	}
	err := shutdownProcesses([]*process{p})
	report.ShutdownVerified = err == nil
	finishRun(ctx, &report, &err)
	if report.Status != "FAIL" || err == nil || !strings.Contains(err.Error(), "exited before requested shutdown") {
		t.Fatalf("late child failure accepted: status=%s err=%v", report.Status, err)
	}
}

func TestRunnerFaultKillMustBeObserved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	victim, _ := startLifecycleChild(t, ctx, "worker")
	if err := victim.killForFault(ctx); err != nil {
		t.Fatal(err)
	}
	if err := shutdownProcesses([]*process{victim}); err != nil {
		t.Fatalf("confirmed injected kill rejected: %v", err)
	}
	failed, release := startLifecycleChild(t, ctx, "worker")
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := failed.await(ctx); err != nil {
		t.Fatal(err)
	}
	if err := failed.killForFault(ctx); err == nil {
		t.Fatal("prior exit treated as injected kill")
	}
}

func TestRunnerShutdownReceipt(t *testing.T) {
	at := time.Now().UTC()
	line := func(when time.Time, msg, cause string) string {
		return fmt.Sprintf("{\"time\":%q,\"level\":\"ERROR\",\"msg\":%q,\"error\":%q}\n", when.Format(time.RFC3339Nano), msg, cause)
	}
	worker := line(at.Add(time.Millisecond), "Worker stopped", "Kafka fetch /-1: context canceled")
	for _, tt := range []struct {
		name, role, log string
		sent            time.Time
		wantReceipt     bool
		wantError       bool
	}{
		{"clean", "master", "", at, false, false},
		{"requested worker cancellation", "worker", worker, at, true, false},
		{"requested master cancellation", "master", line(at, "Master stopped", "context canceled"), at, true, false},
		{"no request", "worker", worker, time.Time{}, false, true},
		{"early cancellation", "worker", worker, at.Add(time.Second), false, true},
		{"panic", "worker", "panic: before exit\n" + worker, at, false, true},
		{"SQL error then cancellation", "worker", line(at, "rollback", "SQLSTATE 55000") + worker, at, false, true},
		{"joined timeout", "worker", line(at, "Worker stopped", "context canceled\ncontext deadline exceeded"), at, false, true},
		{"Kafka partition failure", "worker", line(at, "Worker stopped", "Kafka fetch telemetry.raw/1: broker unavailable"), at, false, true},
		{"replayer error", "replayer", "time=2026-11-17T00:00:00Z level=ERROR msg=failed", time.Time{}, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			allowed, err := shutdownReceipt(strings.NewReader(tt.log), tt.role, tt.sent)
			if allowed != tt.wantReceipt || (err != nil) != tt.wantError {
				t.Fatalf("receipt=%t error=%v", allowed, err)
			}
		})
	}
}

func TestRunnerDeadlinePreventsPass(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	report := Result{Comparison: Comparison{Status: "PASS"}, ShutdownVerified: true}
	var err error
	finishRun(ctx, &report, &err)
	if report.Status != "FAIL" || err == nil {
		t.Fatalf("expired run accepted: status=%s err=%v", report.Status, err)
	}
}

func startLifecycleChild(t *testing.T, ctx context.Context, role string) (*process, string) {
	t.Helper()
	root := t.TempDir()
	release := filepath.Join(root, "release")
	p, err := startProcess(ctx, role, os.Args[0], filepath.Join(root, "child.log"),
		[]string{"-test.run=^TestLifecycleChild$"}, []string{"CAD_LIFECYCLE_TEST_RELEASE=" + release})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.await(bounded); err != nil {
			t.Error(err)
		}
	})
	return p, release
}

func TestLifecycleChild(t *testing.T) {
	file := os.Getenv("CAD_LIFECYCLE_TEST_RELEASE")
	if file == "" {
		return
	}
	for {
		if _, err := os.Stat(file); err == nil {
			os.Exit(1)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
