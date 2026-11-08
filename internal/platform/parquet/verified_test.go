package parquet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func preparedSHA(t testing.TB, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func TestVerifiedParquetSuccessRelocationAndErrors(t *testing.T) {
	path := writeFixture(t, parquetFixtureRow(), true)
	digest := preparedSHA(t, path)
	alias := filepath.Join(t.TempDir(), "relocated.parquet")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(alias, b, 0600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{path, alias} {
		r, v, err := OpenVerifiedParquet(context.Background(), source, digest)
		if err != nil {
			t.Fatal(err)
		}
		record, readErr := r.Next(context.Background())
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || record.PSUPower[0] != 1 || v.SHA256 != digest || v.Bytes != int64(len(b)) || v.Duration < 0 {
			t.Fatal("verified reader changed bytes/values", v, record, readErr, closeErr)
		}
	}
	for _, want := range []string{"", "not-hex", strings.Repeat("a", 63), strings.ToUpper(digest), strings.Repeat("0", 64)} {
		r, _, err := OpenVerifiedParquet(context.Background(), path, want)
		if err == nil || r != nil {
			t.Fatal("invalid/wrong SHA accepted", want, err)
		}
	}
	for _, source := range []string{path + ".missing", t.TempDir()} {
		if r, _, err := OpenVerifiedParquet(context.Background(), source, digest); err == nil || r != nil {
			t.Fatal("unreadable/nonregular source accepted", source, err)
		}
	}
	badSchema := writeFixture(t, parquetFixtureRow(), false)
	if _, _, err := OpenVerifiedParquet(context.Background(), badSchema, preparedSHA(t, badSchema)); err == nil {
		t.Fatal("correct hash bypassed schema validation")
	}
	// On Windows this also proves error cleanup did not retain a no-delete handle.
	if err := os.Rename(badSchema, badSchema+".closed"); err != nil {
		t.Fatal("schema error leaked its source handle", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := OpenVerifiedParquet(ctx, path, digest); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type sourceReaderFunc func([]byte) (int, error)

func (f sourceReaderFunc) Read(b []byte) (int, error) { return f(b) }

func TestSourceHashCancellationReadFailureAndBoundedBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls, largest := 0, 0
	r := sourceReaderFunc(func(p []byte) (int, error) {
		calls++
		largest = max(largest, len(p))
		if calls == 2 {
			cancel()
		}
		return len(p), nil
	})
	digest, n, err := hashPreparedSource(ctx, r)
	if !errors.Is(err, context.Canceled) || digest != "" || calls != 2 || n != 2*sourceHashBufferSize || largest != sourceHashBufferSize {
		t.Fatal("hash ignored cancellation or transfer bound", digest, n, calls, largest, err)
	}
	boom := errors.New("injected source read failure")
	digest, _, err = hashPreparedSource(context.Background(), sourceReaderFunc(func(p []byte) (int, error) { return copy(p, "partial"), boom }))
	if !errors.Is(err, boom) || digest != "" {
		t.Fatal("partial read failure produced a digest", digest, err)
	}
	input := bytes.Repeat([]byte("whole-file"), 10000)
	digest, n, err = hashPreparedSource(context.Background(), bytes.NewReader(input))
	if err != nil || n != int64(len(input)) || digest != fmt.Sprintf("%x", sha256.Sum256(input)) {
		t.Fatal("hash did not cover all bytes", digest, n, err)
	}
}

func TestVerifiedParquetRetainsOriginalHandle(t *testing.T) {
	path := writeFixture(t, parquetFixtureRow(), true)
	r, _, err := OpenVerifiedParquet(context.Background(), path, preparedSHA(t, path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	other := parquetFixtureRow()
	v := 999.0
	other.PSU0Power = &v
	otherPath := writeFixture(t, other, true)
	if err := os.Rename(path, path+".original"); err != nil {
		if runtime.GOOS != "windows" || (!errors.Is(err, os.ErrPermission) && !errors.Is(err, syscall.Errno(32))) {
			t.Fatal(err)
		}
		t.Log("OS_REBIND_DENIED: open source cannot be renamed on Windows; original handle remains valid")
	} else {
		otherBytes, err := os.ReadFile(otherPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, otherBytes, 0600); err != nil {
			t.Fatal(err)
		}
		t.Log("PATH_REPLACED: verified reader must still decode the original opened object")
	}
	record, err := r.Next(context.Background())
	if err != nil || record.PSUPower[0] != 1 {
		t.Fatal("pathname rebound the verified reader", record, err)
	}
}

func TestVerifiedParquetSymlink(t *testing.T) {
	path := writeFixture(t, parquetFixtureRow(), true)
	link := filepath.Join(t.TempDir(), "source-link.parquet")
	if err := os.Symlink(path, link); err != nil {
		if runtime.GOOS == "windows" && (errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.Errno(1314))) {
			t.Skip("symlink creation not permitted in this Windows test environment")
		}
		t.Fatal(err)
	}
	r, _, err := OpenVerifiedParquet(context.Background(), link, preparedSHA(t, path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedParquetDetectsMutationBeforeReadAndClose(t *testing.T) {
	for _, readFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(readFirst), func(t *testing.T) {
			path := writeFixture(t, parquetFixtureRow(), true)
			r, _, err := OpenVerifiedParquet(context.Background(), path, preparedSHA(t, path))
			if err != nil {
				t.Fatal(err)
			}
			if readFirst {
				if _, err := r.Next(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			// Size mutation is deterministic even on filesystems with coarse mtime.
			writer, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				_ = r.Close()
				t.Fatal(err)
			}
			_, writeErr := writer.Write([]byte{0})
			if err := errors.Join(writeErr, writer.Close()); err != nil {
				_ = r.Close()
				t.Fatal(err)
			}
			if !readFirst {
				for i := 0; i < 2; i++ {
					if _, err := r.Next(context.Background()); !errors.Is(err, ErrSourceChanged) {
						t.Fatal("mutated source returned data or failure was not sticky", err)
					}
				}
			}
			if err := r.Close(); !errors.Is(err, ErrSourceChanged) {
				t.Fatal("Close concealed changed source", err)
			}
			if err := os.Rename(path, path+".closed"); err != nil {
				t.Fatal("mutation failure leaked the handle", err)
			}
		})
	}
}

func BenchmarkPreparedSourceSHA(b *testing.B) {
	// A small, cached regular file isolates hash-scan cost, not replay or real D2 I/O.
	path := filepath.Join(b.TempDir(), "hash-source.bin")
	input := bytes.Repeat([]byte{0x5a}, 1<<20)
	if err := os.WriteFile(path, input, 0600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	want := fmt.Sprintf("%x", sha256.Sum256(input))
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		digest, n, err := hashPreparedSource(context.Background(), io.NewSectionReader(file, 0, int64(len(input))))
		if err != nil || digest != want || n != int64(len(input)) {
			b.Fatal(digest, n, err)
		}
	}
}
