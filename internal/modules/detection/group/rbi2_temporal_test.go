package group

import (
	"testing"
	"time"
)

func rbi2Raw(at time.Time, r, b State, localRows bool) Raw {
	raw := Raw{Timestamp: at, Group: "a01", R: RResult{Criterion: Criterion{State: r}}, B: Criterion{State: b}}
	if localRows {
		raw.Local = i2Decisions(at, 18, 18)
	}
	return raw
}

func rbi2Engine(t *testing.T, delta int) (*RBI2Engine, time.Time) {
	t.Helper()
	start := analysisTick()
	e, err := NewRBI2Engine("a01", evalHosts("a01"), delta, start)
	if err != nil {
		t.Fatal(err)
	}
	return e, start
}

func TestVoteRBI2AllThreeValuedCombinations(t *testing.T) {
	states := []State{Unknown, Negative, Positive}
	for _, r := range states {
		for _, b := range states {
			for _, i2 := range states {
				got, err := VoteRBI2(r, b, i2)
				if err != nil {
					t.Fatalf("VoteRBI2(%s,%s,%s): %v", r, b, i2, err)
				}
				p, u := 0, 0
				for _, s := range []State{r, b, i2} {
					if s == Positive {
						p++
					}
					if s == Unknown {
						u++
					}
				}
				want := Unknown
				if p >= 2 {
					want = Positive
				} else if p+u < 2 {
					want = Negative
				}
				if got != want {
					t.Errorf("VoteRBI2(%s,%s,%s)=%s, want %s", r, b, i2, got, want)
				}
			}
		}
	}
	for _, bad := range []State{"", "invalid"} {
		if _, err := VoteRBI2(bad, Negative, Negative); err == nil {
			t.Errorf("accepted invalid state %q", bad)
		}
	}
}

func TestRBI2InclusiveTTLAndUnknownClearsHistory(t *testing.T) {
	for _, delta := range []int{0, 10, 30, 60} {
		e, start := rbi2Engine(t, delta)
		if _, err := e.Step(rbi2Raw(start, Positive, Negative, true)); err != nil {
			t.Fatal(err)
		}
		for elapsed := 10; elapsed < delta; elapsed += 10 {
			if _, err := e.Step(rbi2Raw(start.Add(time.Duration(elapsed)*time.Second), Negative, Negative, false)); err != nil {
				t.Fatal(err)
			}
		}
		if delta > 0 {
			got, err := e.Step(rbi2Raw(start.Add(time.Duration(delta)*time.Second), Negative, Negative, false))
			if err != nil {
				t.Fatal(err)
			}
			if got.R.State != Positive {
				t.Errorf("delta %d inclusive edge R=%s", delta, got.R.State)
			}
		}
		got, err := e.Step(rbi2Raw(start.Add(time.Duration(delta+10)*time.Second), Negative, Negative, false))
		if err != nil {
			t.Fatal(err)
		}
		if got.R.State != Negative {
			t.Errorf("delta %d expiry at delta+10 R=%s", delta, got.R.State)
		}
	}
	e, start := rbi2Engine(t, 30)
	_, _ = e.Step(rbi2Raw(start, Positive, Negative, true))
	got, err := e.Step(rbi2Raw(start.Add(10*time.Second), Unknown, Negative, false))
	if err != nil {
		t.Fatal(err)
	}
	if got.R.State != Unknown {
		t.Fatalf("unknown did not replace positive: %s", got.R.State)
	}
	got, err = e.Step(rbi2Raw(start.Add(20*time.Second), Negative, Positive, false))
	if err != nil {
		t.Fatal(err)
	}
	if got.R.State != Unknown {
		t.Fatalf("unknown history lost before TTL: %s", got.R.State)
	}
	// Even a positive Y value is not one of the three RBI2 votes.
	raw := rbi2Raw(start.Add(30*time.Second), Negative, Negative, false)
	raw.Y.State = Positive
	raw.Local = i2Decisions(raw.Timestamp, 18, 0)
	got, err = e.Step(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.LocalI2.State != Negative || got.Vote != Unknown {
		t.Fatalf("Y changed RBI2 vote: I2=%s vote=%s", got.LocalI2.State, got.Vote)
	}
}

func TestRBI2GapReportsFirstMissingTickAndResetsHistory(t *testing.T) {
	e, start := rbi2Engine(t, 60)
	got, err := e.Step(rbi2Raw(start.Add(20*time.Second), Positive, Negative, true))
	if err != nil {
		t.Fatal(err)
	}
	if got.GapStart == nil || !got.GapStart.Equal(start) {
		t.Fatalf("initial gap start=%v, want %v", got.GapStart, start)
	}
	got, err = e.Step(rbi2Raw(start.Add(40*time.Second), Negative, Positive, false))
	if err != nil {
		t.Fatal(err)
	}
	if got.GapStart == nil || !got.GapStart.Equal(start.Add(30*time.Second)) {
		t.Fatalf("gap start=%v", got.GapStart)
	}
	if got.R.State != Unknown || got.I2.State != Unknown {
		t.Fatalf("gap retained prior positives: R=%s I2=%s", got.R.State, got.I2.State)
	}
}

func TestRBI2FirstEvidenceUsesTightestPairAndLatestTie(t *testing.T) {
	e, start := rbi2Engine(t, 60)
	_, err := e.Step(rbi2Raw(start, Positive, Negative, true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Step(rbi2Raw(start.Add(10*time.Second), Negative, Positive, false))
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Step(rbi2Raw(start.Add(20*time.Second), Positive, Negative, false))
	if err != nil {
		t.Fatal(err)
	}
	if got.Vote != Positive || got.FirstEvidence == nil || !got.FirstEvidence.Equal(start.Add(10*time.Second)) {
		t.Fatalf("vote=%s first=%v", got.Vote, got.FirstEvidence)
	}
	if len(got.ActiveVotes) != 2 || got.ActiveVotes[0] != "R" || got.ActiveVotes[1] != "B" {
		t.Fatalf("active order=%v", got.ActiveVotes)
	}
}

func TestRBI2ValidationDoesNotMutateAndInventoryIsOwned(t *testing.T) {
	hosts := evalHosts("a01")
	e, err := NewRBI2Engine("a01", hosts, 30, analysisTick())
	if err != nil {
		t.Fatal(err)
	}
	start := analysisTick()
	hosts[0] = "mutated"
	bad := rbi2Raw(start, State("bad"), Negative, true)
	if _, err = e.Step(bad); err == nil {
		t.Fatal("invalid state accepted")
	}
	good := rbi2Raw(start, Positive, Negative, true)
	got, err := e.Step(good)
	if err != nil {
		t.Fatalf("valid retry failed: %v", err)
	}
	if got.LocalI2.State != Positive {
		t.Fatalf("constructor did not own inventory: I2=%s", got.LocalI2.State)
	}
	for name, raw := range map[string]Raw{
		"wrong group": {Timestamp: start.Add(10 * time.Second), Group: "a02"},
		"duplicate":   good,
		"unordered":   rbi2Raw(start.Add(-10*time.Second), Negative, Negative, false),
		"invalid R":   rbi2Raw(start.Add(10*time.Second), State("bad"), Negative, false),
	} {
		if _, err := e.Step(raw); err == nil {
			t.Errorf("%s tick accepted", name)
		}
	}
	got, err = e.Step(rbi2Raw(start.Add(10*time.Second), Negative, Positive, false))
	if err != nil {
		t.Fatalf("rejected ticks changed ordering state: %v", err)
	}
	if got.R.State != Positive {
		t.Fatalf("rejected ticks changed vote history: R=%s", got.R.State)
	}
}

func TestRBI2FlushExclusiveBoundary(t *testing.T) {
	e, start := rbi2Engine(t, 10)
	if _, err := e.Step(rbi2Raw(start, Negative, Negative, false)); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(start); err == nil {
		t.Fatal("boundary at last tick accepted")
	}
	if err := e.Flush(start.Add(15 * time.Second)); err == nil {
		t.Fatal("off-grid boundary accepted")
	}
	end := start.Add(10 * time.Second)
	if err := e.Flush(end); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(end); err != nil {
		t.Fatalf("same boundary not idempotent: %v", err)
	}
	if err := e.Flush(end.Add(10 * time.Second)); err == nil {
		t.Fatal("different repeated boundary accepted")
	}
	if _, err := e.Step(rbi2Raw(end, Negative, Negative, false)); err == nil {
		t.Fatal("step after flush accepted")
	}
}
