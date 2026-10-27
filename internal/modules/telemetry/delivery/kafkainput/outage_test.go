package kafkainput

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const outageOwnerLabel = "io.cad.p04-test-owner"

// Each invocation creates its own project. Environment variables select a fresh
// project/port, never authorize adopting a pre-existing Docker resource.
type outageBroker struct {
	project, owner, port, id string
	reservation              string
	env                      []string
}

func (b *outageBroker) endpoint() string { return "localhost:" + b.port }

func (b *outageBroker) docker(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = b.env
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("docker %v: %w: %s%s", args, err, out.String(), stderr.String())
	}
	return out.Bytes(), nil
}

func (b *outageBroker) projectResources(ctx context.Context, kind string) ([]string, error) {
	args := []string{kind, "ls", "-q", "--filter", "label=com.docker.compose.project=" + b.project}
	if kind == "container" {
		args = []string{"ps", "-aq", "--filter", "label=com.docker.compose.project=" + b.project}
	}
	out, err := b.docker(ctx, args...)
	return strings.Fields(string(out)), err
}

func (b *outageBroker) requireFresh(ctx context.Context) error {
	for _, kind := range []string{"container", "volume", "network"} {
		ids, err := b.projectResources(ctx, kind)
		if err != nil {
			return err
		}
		if len(ids) != 0 {
			return fmt.Errorf("refusing existing Compose project %q (%s resources)", b.project, kind)
		}
	}
	// Compose can adopt a named volume/network without a project label. Refuse
	// those names too; applying our labels must never turn foreign data into ours.
	for kind, name := range map[string]string{"volume": b.project + "_kafka-data", "network": b.project + "_default"} {
		out, err := b.docker(ctx, kind, "ls", "--format", "{{.Name}}")
		if err != nil {
			return err
		}
		for _, existing := range strings.Fields(string(out)) {
			if existing == name {
				return fmt.Errorf("refusing pre-existing %s %q", kind, name)
			}
		}
	}
	return nil
}

func newOutageBroker(t *testing.T) *outageBroker {
	t.Helper()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	owner := hex.EncodeToString(token[:])
	project := os.Getenv("P04_KAFKA_OUTAGE_PROJECT")
	if project == "" {
		project = "p04-test-" + owner[:12]
	}
	if !regexp.MustCompile(`^p04-test-[a-z0-9][a-z0-9_-]{0,40}$`).MatchString(project) {
		t.Fatal("P04_KAFKA_OUTAGE_PROJECT must be a fresh p04-test-... project")
	}
	port := os.Getenv("P04_KAFKA_OUTAGE_PORT")
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			t.Fatal("P04_KAFKA_OUTAGE_PORT must be in 1..65535")
		}
	}
	if port == "" {
		port = "0"
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("outage test port must be unused: %v", err)
	}
	port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := filepath.Abs("../../../../../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	b := &outageBroker{project: project, owner: owner, port: port}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		if !strings.HasPrefix(upper, "COMPOSE_") && upper != "KAFKA_HOST_PORT" && upper != "P04_KAFKA_TEST_OWNER" {
			b.env = append(b.env, entry)
		}
	}
	b.env = append(b.env, "KAFKA_HOST_PORT="+port, "P04_KAFKA_TEST_OWNER="+owner)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := b.requireFresh(ctx); err != nil {
		t.Fatal(err) // No mutations have occurred.
	}
	// Docker container names are exclusive. Reserve the project before Compose
	// can adopt/recreate anything, including when two worktrees request one name.
	reservation, err := b.docker(ctx, "create", "--name", project+"-p04-test-lock",
		"--label", outageOwnerLabel+"="+owner, "--label", "io.cad.p04-test-project="+project,
		"--entrypoint", "/bin/true", "apache/kafka:4.1.2")
	if err != nil {
		t.Fatal(err)
	}
	b.reservation = strings.TrimSpace(string(reservation))
	t.Cleanup(func() { b.cleanup(t) }) // Also cleans up a partially failed up.
	if err := b.requireFresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.docker(ctx, "compose", "-f", file, "-p", project, "up", "-d", "--wait", "--wait-timeout", "90", "kafka"); err != nil {
		t.Fatal(err)
	}
	ids, err := b.projectResources(ctx, "container")
	if err != nil || len(ids) != 1 {
		t.Fatalf("expected one owned Kafka container: %v %v", ids, err)
	}
	// Resolve the full immutable ID instead of using a name for stop/start.
	out, err := b.docker(ctx, "inspect", ids[0])
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.verifyContainer(out)
	if err != nil {
		t.Fatal(err)
	}
	b.id = id
	t.Logf("owned outage broker project=%s endpoint=%s container=%s", project, b.endpoint(), id)
	return b
}

func (b *outageBroker) verifyLabels(labels map[string]string) error {
	if labels[outageOwnerLabel] != b.owner || b.owner == "" || labels["com.docker.compose.project"] != b.project {
		return fmt.Errorf("refusing Docker resource: ownership/project mismatch")
	}
	return nil
}

func (b *outageBroker) verifyContainer(raw []byte) (string, error) {
	var rows []struct {
		ID         string `json:"Id"`
		Config     struct{ Labels map[string]string }
		HostConfig struct {
			PortBindings map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string
			}
		}
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 {
		return "", fmt.Errorf("refusing Docker container: unreadable identity")
	}
	r := rows[0]
	if err := b.verifyLabels(r.Config.Labels); err != nil {
		return "", err
	}
	ports := r.HostConfig.PortBindings["9092/tcp"]
	if r.ID == "" || (b.id != "" && r.ID != b.id) || r.Config.Labels["com.docker.compose.service"] != "kafka" ||
		len(ports) != 1 || ports[0].HostIP != "127.0.0.1" || ports[0].HostPort != b.port {
		return "", fmt.Errorf("refusing Docker container: ID/service/loopback endpoint mismatch")
	}
	return r.ID, nil
}

func (b *outageBroker) action(ctx context.Context, action string) error {
	if b.id == "" || (action != "stop" && action != "start") {
		return fmt.Errorf("refusing unowned or unsupported outage action %q", action)
	}
	out, err := b.docker(ctx, "inspect", b.id)
	if err != nil {
		return err
	}
	if _, err := b.verifyContainer(out); err != nil {
		return err
	}
	_, err = b.docker(ctx, action, b.id)
	return err
}

func (b *outageBroker) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	defer func() {
		raw, err := b.docker(ctx, "inspect", b.reservation)
		if err != nil {
			t.Error(err)
			return
		}
		var rows []struct {
			ID     string `json:"Id"`
			Config struct{ Labels map[string]string }
		}
		if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 || rows[0].ID != b.reservation ||
			rows[0].Config.Labels[outageOwnerLabel] != b.owner || rows[0].Config.Labels["io.cad.p04-test-project"] != b.project {
			t.Error("refusing reservation cleanup: ownership mismatch")
			return
		}
		if _, err := b.docker(ctx, "rm", "-v", b.reservation); err != nil {
			t.Error(err)
		}
	}()
	ids, err := b.projectResources(ctx, "container")
	if err != nil {
		t.Error(err)
		return
	}
	for _, candidate := range ids {
		raw, err := b.docker(ctx, "inspect", candidate)
		if err != nil {
			t.Error(err)
			continue
		}
		id, err := b.verifyContainer(raw)
		if err != nil {
			t.Error(err)
			continue
		}
		// -v removes only this verified container's anonymous volumes; named data
		// volume removal below requires its own ownership check.
		if _, err := b.docker(ctx, "rm", "-f", "-v", id); err != nil {
			t.Error(err)
		}
	}
	for _, kind := range []string{"volume", "network"} {
		ids, err := b.projectResources(ctx, kind)
		if err != nil {
			t.Error(err)
			continue
		}
		for _, id := range ids {
			raw, err := b.docker(ctx, kind, "inspect", id)
			if err != nil {
				t.Error(err)
				continue
			}
			var rows []struct{ Labels map[string]string }
			if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 {
				t.Errorf("refusing %s cleanup: unreadable ownership", kind)
				continue
			}
			if err := b.verifyLabels(rows[0].Labels); err != nil {
				t.Error(err)
				continue
			}
			if _, err := b.docker(ctx, kind, "rm", id); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestOutageOwnershipGuard(t *testing.T) {
	b := &outageBroker{project: "p04-test-one", owner: "one", port: "29092", id: "container-one"}
	valid := `[{"Id":"container-one","Config":{"Labels":{"io.cad.p04-test-owner":"one","com.docker.compose.project":"p04-test-one","com.docker.compose.service":"kafka"}},"HostConfig":{"PortBindings":{"9092/tcp":[{"HostIp":"127.0.0.1","HostPort":"29092"}]}}}]`
	if _, err := b.verifyContainer([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"foreign token":      strings.Replace(valid, `"io.cad.p04-test-owner":"one"`, `"io.cad.p04-test-owner":"two"`, 1),
		"foreign project":    strings.ReplaceAll(valid, "p04-test-one", "p04-test-two"),
		"replaced container": strings.ReplaceAll(valid, "container-one", "container-two"),
		"foreign service":    strings.ReplaceAll(valid, `"kafka"`, `"other"`),
		"user broker port":   strings.ReplaceAll(valid, "29092", "9092"),
		"public binding":     strings.ReplaceAll(valid, "127.0.0.1", "0.0.0.0"),
		"unknown identity":   `[]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := b.verifyContainer([]byte(raw)); err == nil {
				t.Fatal("foreign/unknown resource accepted")
			}
		})
	}
	if err := b.action(context.Background(), "rm"); err == nil {
		t.Fatal("unsupported action accepted")
	}
}
