package telemetryarchive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

const d2ID = "f97fa127-bbb9-4fdb-a971-957a44c390a8"
const liveID = "7993d022-3c61-4c53-8891-e1eaa7ff8642"
const summitProfile = "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36"

type Catalog struct {
	ReviewRoot, RunsRoot string
	Client               *http.Client
	mu                   sync.Mutex
	history              map[string]*savedPublication
}

type savedPublication struct {
	Identity Publication
	byGroup  map[string][]time.Time
}

type demoIdentity struct {
	Execution string    `json:"execution"`
	Master    string    `json:"master"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Input     string    `json:"inputSha256"`
	Profile   string    `json:"profileSha256"`
}

func readJSON(path string, limit int64, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fileInfo, err := f.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Size() > limit {
		return fmt.Errorf("invalid archive catalog file")
	}
	return json.NewDecoder(io.LimitReader(f, limit)).Decode(target)
}

func (c *Catalog) Published(ctx context.Context, s Selection) (Publication, error) {
	if !events.ValidExecution(s.Execution) {
		return Publication{}, errUnknownRun
	}
	dir := filepath.Join(c.RunsRoot, s.Execution, "environment")
	legacy := ""
	if s.Execution == d2ID {
		legacy = "d2-max"
	}
	if s.Execution == liveID {
		legacy = "smoke-live"
	}
	if legacy != "" {
		dir = filepath.Join(c.ReviewRoot, "v32-review-snapshots", legacy)
	}
	var demo demoIdentity
	if err := readJSON(filepath.Join(dir, "demo.json"), 32768, &demo); err != nil || demo.Execution != s.Execution {
		return Publication{}, errUnknownRun
	}
	if legacy != "" {
		demo.Input, demo.Profile = SummitSHA, summitProfile
	}
	if !demo.Start.Before(demo.End) || demo.Input == "" || demo.Profile == "" {
		return Publication{}, fmt.Errorf("unverified run identity")
	}
	if _, err := os.Stat(filepath.Join(dir, "verification.json")); err == nil {
		saved, err := c.loadSnapshot(ctx, dir, demo)
		if err != nil {
			return Publication{}, err
		}
		ticks, ok := saved.byGroup[s.Group]
		if !ok {
			return Publication{}, errUnknownRun
		}
		p := saved.Identity
		for _, tick := range ticks {
			if !tick.Before(s.From) && tick.Before(s.To) {
				p.Ticks = append(p.Ticks, tick)
			}
		}
		return p, nil
	} else if !os.IsNotExist(err) {
		return Publication{}, err
	}
	if legacy != "" || demo.Master != "http://127.0.0.1:18086" {
		return Publication{}, errUnknownRun
	}
	query := url.Values{"execution_id": {s.Execution}, "group_id": {s.Group},
		"from": {s.From.UTC().Format(time.RFC3339)}, "to": {s.To.UTC().Format(time.RFC3339)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, demo.Master+publicationPath+"?"+query.Encode(), nil)
	if err != nil {
		return Publication{}, err
	}
	reply, err := c.Client.Do(req)
	if err != nil {
		return Publication{}, err
	}
	defer reply.Body.Close()
	if reply.StatusCode == 404 {
		// A registered entry may exist while its Master is still being prepared.
		return Publication{}, fmt.Errorf("Master publication unavailable")
	}
	if reply.StatusCode != 200 {
		return Publication{}, fmt.Errorf("Master publication unavailable")
	}
	var p Publication
	if err := json.NewDecoder(io.LimitReader(reply.Body, 16*1024)).Decode(&p); err != nil {
		return Publication{}, err
	}
	if p.Execution != demo.Execution || p.Source != demo.Input || p.Profile != demo.Profile ||
		!p.Start.Equal(demo.Start) || !p.End.Equal(demo.End) || len(p.Ticks) > 61 {
		return Publication{}, fmt.Errorf("Master identity differs from catalog")
	}
	return p, nil
}

func (c *Catalog) loadSnapshot(ctx context.Context, dir string, demo demoIdentity) (*savedPublication, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reused := c.history[demo.Execution]; reused != nil {
		return reused, nil
	}
	var manifest struct {
		Summary  events.Summary `json:"summary"`
		Snapshot struct {
			ResultPages int `json:"resultPages"`
		} `json:"snapshot"`
	}
	if err := readJSON(filepath.Join(dir, "verification.json"), 1<<20, &manifest); err != nil {
		return nil, err
	}
	s := manifest.Summary
	if !s.Complete || s.ExecutionID != demo.Execution || !s.WindowStart.Equal(demo.Start) ||
		!s.WindowEnd.Equal(demo.End) || (s.GroupCount != 4 && s.GroupCount != 8 && s.GroupCount != 16) ||
		s.ExpectedTicks != int64(s.GroupCount)*int64(s.WindowEnd.Sub(s.WindowStart)/(10*time.Second)) ||
		s.Ticks != s.ExpectedTicks || manifest.Snapshot.ResultPages < 1 || manifest.Snapshot.ResultPages > 360 {
		return nil, fmt.Errorf("snapshot failed manifest validation")
	}
	p := &savedPublication{Identity: Publication{Execution: demo.Execution, Source: demo.Input,
		Profile: demo.Profile, Start: demo.Start, End: demo.End}, byGroup: make(map[string][]time.Time)}
	tick := demo.Start
	group := 1
	var count int64
	for i := 0; i < manifest.Snapshot.ResultPages; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var page struct {
			Items []struct {
				Raw struct {
					Group     string    `json:"group"`
					Timestamp time.Time `json:"timestamp"`
				} `json:"raw"`
			} `json:"items"`
		}
		path := filepath.Join(dir, "snapshot", "results-"+strconv.Itoa(i)+".json")
		if err := readJSON(path, 24<<20, &page); err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if item.Raw.Group != fmt.Sprintf("a%02d", group) || !item.Raw.Timestamp.Equal(tick) || !tick.Before(demo.End) {
				return nil, fmt.Errorf("snapshot has a missing or foreign tick")
			}
			p.byGroup[item.Raw.Group] = append(p.byGroup[item.Raw.Group], tick)
			count++
			group++
			if group > s.GroupCount {
				group = 1
				tick = tick.Add(10 * time.Second)
			}
		}
	}
	if count != s.Ticks {
		return nil, fmt.Errorf("snapshot count differs from manifest")
	}
	if len(c.history) >= 16 {
		clear(c.history)
	}
	if c.history == nil {
		c.history = make(map[string]*savedPublication)
	}
	c.history[demo.Execution] = p
	return p, nil
}
