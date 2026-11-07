package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProfileOptInAndLoopback(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		value := "0"
		if enabled {
			value = "1"
		}
		t.Setenv("CAD_PPROF", value)
		h := New().Handler(http.NotFoundHandler())
		for _, remote := range []string{"127.0.0.1:1234", "192.0.2.1:1234", "invalid"} {
			q := httptest.NewRequest("GET", "/debug/pprof/goroutine?debug=1", nil)
			q.RemoteAddr = remote
			w := httptest.NewRecorder()
			h.ServeHTTP(w, q)
			want := 404
			if enabled {
				want = 403
				if remote == "127.0.0.1:1234" {
					want = 200
				}
			}
			if w.Code != want {
				t.Fatalf("enabled=%v remote=%s got=%d want=%d", enabled, remote, w.Code, want)
			}
		}
	}
}
