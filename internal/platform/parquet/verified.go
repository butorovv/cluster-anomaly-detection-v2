package parquet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

var ErrSourceMismatch = errors.New("prepared source SHA256 mismatch")
var ErrSourceChanged = errors.New("prepared source changed during verification or replay")

const sourceHashBufferSize = 64 << 10

// SourceVerification describes setup work, not detector/replay throughput.
type SourceVerification struct {
	SHA256   string
	Bytes    int64
	Duration time.Duration // hash scan only; excludes schema parsing
}

// OpenVerifiedParquet hashes the full regular file before schema parsing and
// retains that same handle for replay. The source must remain immutable; metadata
// guards detect ordinary mutations, not a writer that restores size/timestamps.
func OpenVerifiedParquet(ctx context.Context, path, expectedSHA string) (*ParquetReader, SourceVerification, error) {
	var verified SourceVerification
	if err := ctx.Err(); err != nil {
		return nil, verified, err
	}
	if b, err := hex.DecodeString(expectedSHA); err != nil || len(b) != sha256.Size || expectedSHA != strings.ToLower(expectedSHA) {
		return nil, verified, fmt.Errorf("trusted prepared source SHA256 must be 64 lowercase hex characters")
	}
	// Avoid opening obvious FIFOs/devices. A hostile path race is outside the
	// immutable local dataset contract; validate the actual opened object too.
	info, err := os.Stat(path)
	if err != nil {
		return nil, verified, fmt.Errorf("stat prepared source: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, verified, fmt.Errorf("prepared source must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, verified, fmt.Errorf("open prepared source: %w", err)
	}
	fail := func(err error) (*ParquetReader, SourceVerification, error) {
		return nil, verified, errors.Join(err, file.Close())
	}
	info, err = file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(fmt.Errorf("prepared source must be a regular file"))
	}
	started := time.Now()
	verified.SHA256, verified.Bytes, err = hashPreparedSource(ctx, io.NewSectionReader(file, 0, info.Size()))
	verified.Duration = time.Since(started)
	if err != nil {
		return fail(fmt.Errorf("hash prepared source: %w", err))
	}
	if verified.Bytes != info.Size() {
		return fail(io.ErrUnexpectedEOF)
	}
	if err := unchangedSource(file, info); err != nil {
		return fail(err)
	}
	if verified.SHA256 != expectedSHA {
		return fail(ErrSourceMismatch)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	reader, err := openParquetFile(file) // ownership transferred, never reopen path
	if err != nil {
		return nil, verified, err
	}
	reader.verifiedFile = info
	if err := reader.checkSourceUnchanged(); err != nil {
		return nil, verified, errors.Join(err, reader.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, verified, errors.Join(err, reader.Close())
	}
	return reader, verified, nil
}

type sourceContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r sourceContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.source.Read(p)
	if canceled := r.ctx.Err(); canceled != nil {
		return n, canceled
	}
	return n, err
}

func hashPreparedSource(ctx context.Context, source io.Reader) (string, int64, error) {
	h := sha256.New()
	// The wrapper exposes no WriterTo, so CopyBuffer cannot bypass cancellation
	// checks or silently choose a different transfer buffer.
	n, err := io.CopyBuffer(h, sourceContextReader{ctx: ctx, source: source}, make([]byte, sourceHashBufferSize))
	if err != nil {
		return "", n, err
	}
	if err := ctx.Err(); err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func unchangedSource(file *os.File, before os.FileInfo) error {
	after, err := file.Stat()
	if err != nil {
		return fmt.Errorf("check prepared source: %w", err)
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return ErrSourceChanged
	}
	return nil
}

func (r *ParquetReader) checkSourceUnchanged() error {
	if r.verifiedFile == nil {
		return nil
	}
	return unchangedSource(r.file, r.verifiedFile)
}
