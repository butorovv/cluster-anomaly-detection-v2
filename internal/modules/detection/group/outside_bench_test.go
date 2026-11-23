package group_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func outsideRows(n int) []group.Raw {
	rows := make([]group.Raw, n)
	for i := range rows {
		available, anomalous := 18-i%7, i%5
		score := float64(anomalous) / float64(available)
		rows[i] = group.Raw{Group: fmt.Sprintf("a%03d", i+1), Timestamp: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC),
			Complete: available, IF: group.Support{Available: available, Anomalous: anomalous, S: &score}}
		if i%11 == 0 {
			rows[i].IF.Available, rows[i].IF.Anomalous, rows[i].IF.S = 0, 0, nil
		}
	}
	return rows
}

func BenchmarkAnnotateOutside(b *testing.B) {
	for _, n := range []int{16, 64, 200} {
		for _, variant := range outsideCalculations {
			b.Run(fmt.Sprintf("%d/%s", n, variant.name), func(b *testing.B) {
				rows := outsideRows(n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					variant.annotate(rows)
				}
			})
		}
	}
}

var outsideCalculations = []struct {
	name     string
	annotate func([]group.Raw)
}{{"Before", annotateOutsideReference}, {"After", group.AnnotateOutside}}

func BenchmarkOutsideSerialization(b *testing.B) {
	for _, n := range []int{16, 64, 200} {
		for _, variant := range outsideCalculations {
			b.Run(fmt.Sprintf("%d/%s", n, variant.name), func(b *testing.B) {
				rows := outsideRows(n)
				variant.annotate(rows)
				payloads := make([]domain.GroupResult, n)
				for i, raw := range rows {
					payloads[i] = domain.GroupResult{Raw: raw, Evidence: &group.RBI2Evidence{Raw: raw}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, payload := range payloads {
						if _, err := json.Marshal(payload); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
