package group

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// Config contains the frozen fields checked by the offline CLI. Additional
// descriptive fields in its historical JSON remain unchanged and SHA-bound.
type Config struct {
	Version    string  `json:"version"`
	Floor      float64 `json:"mad_floor"`
	RCond      float64 `json:"svd_rcond"`
	Dimensions int     `json:"dimensions"`
	DDOF       int     `json:"covariance_ddof"`
	Quantile   float64 `json:"quantile"`
	Delta      []int   `json:"delta_seconds"`
	Policy     string  `json:"y_unknown_policy"`
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Version != Version || c.Floor != Floor || c.RCond != RCond || c.Dimensions != Dimensions || c.DDOF != 1 || c.Quantile != .99 || c.Policy != "preliminary_rb" || !slices.Equal(c.Delta, []int{0, 10, 30, 60}) {
		return nil, fmt.Errorf("config differs from implemented frozen contract")
	}
	return &c, nil
}
