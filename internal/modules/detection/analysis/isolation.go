package analysis

import (
	"context"
	"fmt"
	"math"
	"math/rand"
)

const isolationForestVersion = "isolation-forest-v1"

type IsolationConfig struct {
	Trees      int   `json:"trees"`
	SampleSize int   `json:"sample_size"`
	MaxDepth   int   `json:"max_depth"`
	Seed       int64 `json:"seed"`
}

func DefaultIsolationConfig() IsolationConfig {
	return IsolationConfig{Trees: 100, SampleSize: 256, MaxDepth: 8, Seed: 42}
}

func (c IsolationConfig) Validate() error {
	if c.Trees < 1 || c.Trees > 10000 || c.SampleSize < 2 || c.SampleSize > 65536 || c.MaxDepth < 0 || c.MaxDepth > 64 || int64(c.Trees)*int64(c.SampleSize) > 1e7 {
		return fmt.Errorf("invalid isolation forest configuration")
	}
	return nil
}

type IsolationNode struct {
	Size    int            `json:"size"`
	Feature int            `json:"feature"`
	Split   float64        `json:"split"`
	Left    *IsolationNode `json:"left,omitempty"`
	Right   *IsolationNode `json:"right,omitempty"`
}

type IsolationForest struct {
	Version               string           `json:"version"`
	TrainingWindow        Window           `json:"training_window"`
	Observations          int              `json:"observations"`
	EffectiveSampleSize   int              `json:"effective_sample_size"`
	Config                IsolationConfig  `json:"config"`
	Median                Vector           `json:"median"`
	MAD                   Vector           `json:"mad"`
	Scale                 Vector           `json:"effective_scale"`
	Trees                 []*IsolationNode `json:"trees"`
	corrections           []float64
	normalizingCorrection float64
}

// FitIsolation trains independent trees from Algorithm R reservoirs in a single D1 pass.
func FitIsolation(ctx context.Context, data Data, normalizer *StatisticalProfile, window Window, cfg IsolationConfig) (*IsolationForest, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil || data == nil || normalizer == nil || window != NIRWindows()[0] {
		return nil, fmt.Errorf("isolation training requires data, profile, and fixed D1 window")
	}
	if err := normalizer.Validate(); err != nil {
		return nil, fmt.Errorf("invalid normalization profile: %w", err)
	}
	if normalizer.TrainingWindow != NIRWindows()[0] {
		return nil, fmt.Errorf("normalization profile is not trained on D1")
	}
	f := &IsolationForest{Version: isolationForestVersion, TrainingWindow: window, Config: cfg, Median: normalizer.Median, MAD: normalizer.MAD, Scale: normalizer.Scale}
	effective := cfg.SampleSize
	reservoirs := make([][]Vector, cfg.Trees)
	rngs := make([]*rand.Rand, cfg.Trees)
	master := rand.New(rand.NewSource(cfg.Seed))
	for i := 0; i < cfg.Trees; i++ {
		reservoirs[i] = make([]Vector, 0, cfg.SampleSize)
		rngs[i] = rand.New(rand.NewSource(master.Int63()))
	}
	if err := data(ctx, func(o Observation) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !window.Contains(o.Timestamp) {
			return fmt.Errorf("non-D1 observation in isolation training")
		}
		z, err := normalizer.Normalize(o.Features)
		if err != nil {
			return err
		}
		f.Observations++
		for i := range reservoirs {
			r := reservoirs[i]
			if len(r) < cfg.SampleSize {
				reservoirs[i] = append(r, z)
				continue
			}
			j := rngs[i].Intn(f.Observations)
			if j < cfg.SampleSize {
				r[j] = z
			}
		}
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	if f.Observations < 2 {
		return nil, fmt.Errorf("isolation training requires at least two D1 states")
	}
	if f.Observations < effective {
		effective = f.Observations
	}
	f.EffectiveSampleSize = effective
	f.Trees = make([]*IsolationNode, cfg.Trees)
	for i := range f.Trees {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		f.Trees[i], err = buildIsolationTree(ctx, reservoirs[i], 0, cfg.MaxDepth, rngs[i])
		if err != nil {
			return nil, err
		}
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return f, nil
}

func buildIsolationTree(ctx context.Context, rows []Vector, depth, maxDepth int, rng *rand.Rand) (*IsolationNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := &IsolationNode{Size: len(rows), Feature: -1}
	if depth >= maxDepth || len(rows) <= 1 {
		return n, nil
	}
	var eligible []int
	var mins, maxs Vector
	for j := 0; j < Dimensions; j++ {
		mins[j], maxs[j] = rows[0][j], rows[0][j]
		for _, row := range rows[1:] {
			if row[j] < mins[j] {
				mins[j] = row[j]
			}
			if row[j] > maxs[j] {
				maxs[j] = row[j]
			}
		}
		if mins[j] < maxs[j] {
			eligible = append(eligible, j)
		}
	}
	if len(eligible) == 0 {
		return n, nil
	}
	feature := eligible[rng.Intn(len(eligible))]
	lo, hi := mins[feature], maxs[feature]
	u := rng.Float64()
	split := lo*(1-u) + hi*u
	if !Finite(split) {
		split = lo/2 + hi/2
	}
	if split <= lo {
		split = math.Nextafter(lo, hi)
	}
	if split > hi {
		split = hi
	}
	if split <= lo {
		split = hi
	}
	left, right := make([]Vector, 0, len(rows)), make([]Vector, 0, len(rows))
	for _, row := range rows {
		if row[feature] < split {
			left = append(left, row)
		} else {
			right = append(right, row)
		}
	}
	if len(left) == 0 || len(right) == 0 {
		return n, nil
	}
	n.Feature, n.Split = feature, split
	var err error
	n.Left, err = buildIsolationTree(ctx, left, depth+1, maxDepth, rng)
	if err != nil {
		return nil, err
	}
	n.Right, err = buildIsolationTree(ctx, right, depth+1, maxDepth, rng)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// IsolationPathCorrection returns c(n), with exact harmonic summation.
func IsolationPathCorrection(n int) float64 {
	if n <= 1 {
		return 0
	}
	if n == 2 {
		return 1
	}
	h := 0.0
	for k := 1; k < n; k++ {
		h += 1 / float64(k)
	}
	return 2*h - 2*float64(n-1)/float64(n)
}

func (f *IsolationForest) Score(v Vector) (float64, error) {
	if f == nil || f.EffectiveSampleSize < 2 || len(f.Trees) == 0 {
		return 0, fmt.Errorf("invalid isolation forest")
	}
	p := &StatisticalProfile{Median: f.Median, MAD: f.MAD, Scale: f.Scale}
	z, err := p.Normalize(v)
	if err != nil {
		return 0, err
	}
	correction := func(n int) float64 {
		if n >= 0 && n < len(f.corrections) {
			return f.corrections[n]
		}
		return IsolationPathCorrection(n)
	}
	total := 0.0
	for _, root := range f.Trees {
		depth, node := 0, root
		if node == nil {
			return 0, fmt.Errorf("nil isolation tree")
		}
		for node.Left != nil || node.Right != nil {
			if node.Left == nil || node.Right == nil || node.Feature < 0 || node.Feature >= Dimensions || !Finite(node.Split) {
				return 0, fmt.Errorf("malformed isolation node")
			}
			if z[node.Feature] < node.Split {
				node = node.Left
			} else {
				node = node.Right
			}
			depth++
			if node == nil || depth > f.Config.MaxDepth {
				return 0, fmt.Errorf("malformed isolation path")
			}
		}
		if node.Feature != -1 || node.Size < 1 {
			return 0, fmt.Errorf("malformed isolation leaf")
		}
		total += float64(depth) + correction(node.Size)
	}
	mean := total / float64(len(f.Trees))
	denom := f.normalizingCorrection
	if denom <= 0 {
		denom = IsolationPathCorrection(f.EffectiveSampleSize)
	}
	score := math.Exp2(-mean / denom)
	if !Finite(score) {
		return 0, fmt.Errorf("nonfinite isolation score")
	}
	return score, nil
}

func (f *IsolationForest) Validate() error {
	if f == nil || f.Version != isolationForestVersion || f.TrainingWindow != NIRWindows()[0] || f.Observations < 2 || f.EffectiveSampleSize < 2 || f.EffectiveSampleSize > f.Observations || f.EffectiveSampleSize > f.Config.SampleSize || len(f.Trees) != f.Config.Trees {
		return fmt.Errorf("invalid isolation forest metadata")
	}
	if err := f.Config.Validate(); err != nil {
		return err
	}
	if f.EffectiveSampleSize != minInt(f.Observations, f.Config.SampleSize) {
		return fmt.Errorf("effective isolation sample size disagrees with metadata")
	}
	for j := 0; j < Dimensions; j++ {
		if !Finite(f.Median[j]) || !Finite(f.MAD[j]) || f.MAD[j] < 0 || !Finite(f.Scale[j]) || f.Scale[j] != math.Max(1.4826*f.MAD[j], 0.0001) {
			return fmt.Errorf("invalid isolation profile coordinate %d", j)
		}
	}
	for _, root := range f.Trees {
		seen := make(map[*IsolationNode]bool)
		nodes := 0
		var walk func(*IsolationNode, int) (int, error)
		walk = func(n *IsolationNode, depth int) (int, error) {
			if n == nil || seen[n] {
				return 0, fmt.Errorf("nil, cyclic, or shared isolation node")
			}
			seen[n] = true
			nodes++
			if nodes > 2*f.EffectiveSampleSize-1 || depth > f.Config.MaxDepth || n.Size < 1 || n.Size > f.EffectiveSampleSize {
				return 0, fmt.Errorf("invalid isolation tree size or depth")
			}
			if n.Left == nil && n.Right == nil {
				if n.Feature != -1 || n.Split != 0 {
					return 0, fmt.Errorf("invalid isolation leaf")
				}
				return n.Size, nil
			}
			if n.Left == nil || n.Right == nil || n.Feature < 0 || n.Feature >= Dimensions || !Finite(n.Split) {
				return 0, fmt.Errorf("invalid isolation branch")
			}
			l, err := walk(n.Left, depth+1)
			if err != nil {
				return 0, err
			}
			r, err := walk(n.Right, depth+1)
			if err != nil {
				return 0, err
			}
			if l+r != n.Size {
				return 0, fmt.Errorf("isolation child sizes do not conserve parent")
			}
			return n.Size, nil
		}
		if root == nil || root.Size != f.EffectiveSampleSize {
			return fmt.Errorf("isolation root size disagrees with sample size")
		}
		if _, err := walk(root, 0); err != nil {
			return err
		}
	}
	f.corrections = make([]float64, f.EffectiveSampleSize+1)
	h := 0.0
	for n := 2; n < len(f.corrections); n++ {
		h += 1 / float64(n-1)
		f.corrections[n] = 2*h - 2*float64(n-1)/float64(n)
	}
	f.normalizingCorrection = f.corrections[f.EffectiveSampleSize]
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
