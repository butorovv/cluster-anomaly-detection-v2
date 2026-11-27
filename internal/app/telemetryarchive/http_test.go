package telemetryarchive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
)

type publicationFixture struct{ result Publication }

func (f publicationFixture) Published(context.Context, Selection) (Publication, error) {
	return f.result, nil
}

type measurementsFixture struct {
	queried    []time.Time
	start, end time.Time
}

func (f *measurementsFixture) Imported(context.Context, string, string) ([]archive.ImportRange, error) {
	return []archive.ImportRange{{From: f.start, To: f.end}}, nil
}
func (f *measurementsFixture) Samples(_ context.Context, _ string, ticks []time.Time) (map[int64]archive.Values, error) {
	f.queried = append(f.queried, ticks...)
	result := map[int64]archive.Values{}
	if len(ticks) > 0 {
		zero := 0.0
		result[ticks[0].Unix()] = archive.Values{&zero}
	}
	return result, nil
}

func TestTelemetryPublicationAndCoverage(t *testing.T) {
	at := summitWindow.Start
	end := at.Add(time.Minute)
	f := &measurementsFixture{start: at, end: at.Add(30 * time.Second)}
	api := Handler(publicationFixture{Publication{Execution: d2ID, Source: SummitSHA, Profile: summitProfile, Start: at, End: end,
		Ticks: []time.Time{at, at.Add(20 * time.Second), at.Add(30 * time.Second)}}}, f, nil)
	query := url.Values{"execution_id": {d2ID}, "group_id": {"a14"}, "host_id": {"a14n04"},
		"from": {at.Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/telemetry?"+query.Encode(), nil))
	if response.Code != 200 {
		t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
	}
	var data Response
	if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	var statuses []string
	for _, sample := range data.Items {
		statuses = append(statuses, sample.Status)
	}
	if want := []string{"available", "not_published", "missing_observation", "pending_archive", "not_published", "not_published"}; !reflect.DeepEqual(statuses, want) {
		t.Fatalf("status = %v", statuses)
	}
	if len(f.queried) != 2 || !f.queried[0].Equal(at) || !f.queried[1].Equal(at.Add(20*time.Second)) {
		t.Fatalf("queries contain unauthorized timestamps: %v", f.queried)
	}
	values := data.Items[0].Values
	if values == nil || values[0] == nil || *values[0] != 0 || values[1] != nil || data.Channels[10].Unit != "°C" {
		t.Fatal("zero, null or physical unit was lost")
	}
	query.Set("host_id", "a13n04")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/telemetry?"+query.Encode(), nil))
	if response.Code != 400 {
		t.Fatalf("foreign host accepted: %d", response.Code)
	}
}
