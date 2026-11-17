package experiment

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

type process struct {
	cmd             *exec.Cmd
	role, logPath   string
	done            chan struct{}
	err, logErr     error
	faultKilled     bool
	owner, endpoint string
}

func startProcess(ctx context.Context, role, binary, logPath string, args, env []string) (*process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	// Only the lifecycle owner signals children. CommandContext would race
	// cleanup with an unclassified kill at the run deadline.
	cmd := exec.Command(binary, args...)
	prepareProcess(cmd)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = f, f
	p := &process{cmd: cmd, role: role, logPath: logPath, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	go func() {
		p.err = cmd.Wait()
		p.logErr = f.Close()
		close(p.done)
	}()
	return p, nil
}

func (p *process) failure() error {
	select {
	case <-p.done:
		return errors.Join(fmt.Errorf("%s exited before requested shutdown (pid %d)", p.role, p.cmd.Process.Pid), p.err)
	default:
		return nil
	}
}

func (p *process) await(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *process) killForFault(ctx context.Context) error {
	if err := p.failure(); err != nil {
		return err
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("inject %s kill: %w", p.role, err)
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.await(bounded); err != nil {
		return fmt.Errorf("reap injected %s kill: %w", p.role, err)
	}
	if p.cmd.ProcessState == nil || !faultExit(p.cmd.ProcessState) {
		return errors.Join(fmt.Errorf("%s did not exit from the injected kill", p.role), p.err)
	}
	p.faultKilled = true
	return nil
}

func shutdownProcesses(children []*process) error {
	grace, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var stopErr error
	var unreaped []*process
	completed := make(map[*process]time.Time)
	for _, role := range []string{"replayer", "worker", "master"} {
		requested := make(map[*process]time.Time)
		for _, p := range children {
			if p.role != role {
				continue
			}
			if early := p.failure(); early != nil {
				if role == "replayer" || p.faultKilled {
					completed[p] = time.Time{}
				} else {
					stopErr = errors.Join(stopErr, early)
				}
				continue
			}
			if role == "replayer" {
				stopErr = errors.Join(stopErr, fmt.Errorf("replayer still running at shutdown"))
			}
			at := time.Now()
			if err := signalProcess(p.cmd.Process); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("signal %s: %w", role, err))
				unreaped = append(unreaped, p)
				continue
			}
			requested[p] = at
		}
		for p, at := range requested {
			if err := p.await(grace); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s shutdown timeout: %w", p.role, err))
				unreaped = append(unreaped, p)
				continue
			}
			completed[p] = at
		}
	}
	for _, p := range unreaped {
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			stopErr = errors.Join(stopErr, fmt.Errorf("kill %s: %w", p.role, err))
		}
	}
	reap, cancelReap := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReap()
	for _, p := range unreaped {
		if err := p.await(reap); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("reap %s: %w", p.role, err))
		}
	}
	for _, p := range children {
		if at, ok := completed[p]; ok {
			stopErr = errors.Join(stopErr, p.verifyExit(at))
		}
	}
	return stopErr
}

func (p *process) verifyExit(sent time.Time) error {
	if p.logErr != nil {
		return fmt.Errorf("close %s log: %w", p.role, p.logErr)
	}
	f, err := os.Open(p.logPath)
	if err != nil {
		return err
	}
	defer f.Close()
	code := p.cmd.ProcessState.ExitCode()
	receipt, err := shutdownReceipt(f, p.role, sent)
	if err != nil {
		return fmt.Errorf("%s exit (code %d): %w", p.role, code, err)
	}
	if p.faultKilled || code == 0 && p.err == nil && !receipt {
		return nil
	}
	if receipt && (p.role == "worker" && code == 130 || p.role == "master" && code == 1) {
		return nil
	}
	return errors.Join(fmt.Errorf("unexpected %s exit code %d", p.role, code), p.err)
}

var kafkaCanceled = regexp.MustCompile(`^Kafka fetch [A-Za-z0-9._/-]+: context canceled$`)
var inputCanceled = regexp.MustCompile(`^handle telemetry\.raw/[0-9]+@[0-9]+: context canceled$`)

// As in LIFECYCLE-4, verify the timestamp and entire error chain against
// the requested stop. Any other ERROR, including one on a zero exit, is fatal.
func shutdownReceipt(log io.Reader, role string, sent time.Time) (bool, error) {
	scan := bufio.NewScanner(log)
	scan.Buffer(make([]byte, 4096), 1<<20)
	found := false
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" {
			continue
		}
		if found {
			return false, fmt.Errorf("output after the shutdown receipt")
		}
		var entry struct {
			At    time.Time   `json:"time"`
			Level *slog.Level `json:"level"`
			Msg   string      `json:"msg"`
			Error string      `json:"error"`
		}
		if err := json.Unmarshal(scan.Bytes(), &entry); err != nil || entry.Level == nil {
			if role == "replayer" && strings.HasPrefix(line, "time=") &&
				!strings.Contains(line, " level=ERROR ") && !strings.Contains(line, " level=FATAL ") {
				continue
			}
			return false, fmt.Errorf("unexpected output in %s log", role)
		}
		if *entry.Level < slog.LevelError {
			continue
		}
		if sent.IsZero() || entry.At.Before(sent) || *entry.Level != slog.LevelError ||
			!(role == "worker" && entry.Msg == "Worker stopped" || role == "master" && entry.Msg == "Master stopped") {
			return false, fmt.Errorf("unexpected runtime error: %s", entry.Msg)
		}
		for _, cause := range strings.Split(entry.Error, "\n") {
			if cause != "context canceled" && !kafkaCanceled.MatchString(cause) && !inputCanceled.MatchString(cause) {
				return false, fmt.Errorf("shutdown error is not cancellation-only: %s", entry.Msg)
			}
		}
		found = true
	}
	return found, scan.Err()
}

func finishRun(ctx context.Context, report *Result, runErr *error) {
	*runErr = errors.Join(*runErr, ctx.Err())
	if *runErr == nil && (!report.ShutdownVerified || report.Comparison.Status != "PASS") {
		*runErr = fmt.Errorf("run lacks verified comparison and child shutdown")
	}
	if *runErr == nil {
		report.Status = "PASS"
	} else {
		report.Status = "FAIL"
	}
}
