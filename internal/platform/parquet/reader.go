package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const PreparedSchemaVersion = "F18-v1"

type preparedRow struct {
	Timestamp        int64    `parquet:"timestamp,timestamp(microsecond:utc)"`
	Hostname         string   `parquet:"hostname"`
	GroupID          string   `parquet:"group_id"`
	NodeIndex        int32    `parquet:"node_index,int(32)"`
	PSU0Power        *float64 `parquet:"psu0_power,optional"`
	PSU1Power        *float64 `parquet:"psu1_power,optional"`
	CPU0Power        *float64 `parquet:"cpu0_power,optional"`
	CPU1Power        *float64 `parquet:"cpu1_power,optional"`
	GPU0Power        *float64 `parquet:"gpu0_power,optional"`
	GPU1Power        *float64 `parquet:"gpu1_power,optional"`
	GPU2Power        *float64 `parquet:"gpu2_power,optional"`
	GPU3Power        *float64 `parquet:"gpu3_power,optional"`
	GPU4Power        *float64 `parquet:"gpu4_power,optional"`
	GPU5Power        *float64 `parquet:"gpu5_power,optional"`
	CPU0Temp         *float64 `parquet:"cpu0_mean_temp,optional"`
	CPU1Temp         *float64 `parquet:"cpu1_mean_temp,optional"`
	GPU0Temp         *float64 `parquet:"gpu0_core_temp,optional"`
	GPU1Temp         *float64 `parquet:"gpu1_core_temp,optional"`
	GPU2Temp         *float64 `parquet:"gpu2_core_temp,optional"`
	GPU3Temp         *float64 `parquet:"gpu3_core_temp,optional"`
	GPU4Temp         *float64 `parquet:"gpu4_core_temp,optional"`
	GPU5Temp         *float64 `parquet:"gpu5_core_temp,optional"`
	Valid            bool     `parquet:"is_valid"`
	AvailabilityMask uint32   `parquet:"availability_mask"`
	CPU0ValidCores   int32    `parquet:"cpu0_valid_cores,int(32)"`
	CPU1ValidCores   int32    `parquet:"cpu1_valid_cores,int(32)"`
}

type ParquetReader struct {
	file         *os.File
	reader       *parquet.GenericReader[preparedRow]
	buffer       []preparedRow
	index, count int
	pending      error
	rows         int64
	firstDay     string
}

func OpenParquet(path string) (*ParquetReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open prepared parquet: %w", err)
	}
	fail := func(err error) (*ParquetReader, error) { _ = file.Close(); return nil, err }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	pf, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return fail(fmt.Errorf("parquet footer: %w", err))
	}
	version, ok := pf.Lookup("schema_version")
	if !ok || version != PreparedSchemaVersion {
		return fail(fmt.Errorf("prepared schema_version must be %s", PreparedSchemaVersion))
	}
	expected := parquet.SchemaOf(preparedRow{})
	actualFields := pf.Schema().Fields()
	if len(actualFields) != len(expected.Fields()) {
		return fail(fmt.Errorf("prepared column count differs from F18-v1"))
	}
	for _, want := range expected.Fields() {
		found := false
		for _, got := range actualFields {
			if got.Name() != want.Name() {
				continue
			}
			found = true
			if !got.Leaf() || got.Repeated() || got.Optional() != want.Optional() || !compatibleType(got.Type(), want.Type()) {
				return fail(fmt.Errorf("prepared column %s has incompatible type/nullability: got %s (optional=%t), expected %s (optional=%t)", want.Name(), got.Type().String(), got.Optional(), want.Type().String(), want.Optional()))
			}
		}
		if !found {
			return fail(fmt.Errorf("prepared column missing: %s", want.Name()))
		}
	}
	if pf.NumRows() == 0 {
		return fail(fmt.Errorf("empty prepared parquet"))
	}
	return &ParquetReader{file: file, reader: parquet.NewGenericReader[preparedRow](pf), buffer: make([]preparedRow, 256), rows: pf.NumRows()}, nil
}

func (r *ParquetReader) Rows() int64 { return r.rows }

// parquet-go canonicalizes a file's signed INT(32,true) to its physical INT32.
// These are equivalent; unsigned integers, timestamp units and nullability are not.
func compatibleType(got, want parquet.Type) bool {
	if got.Kind() != want.Kind() {
		return false
	}
	if got.String() == want.String() {
		return true
	}
	signedInt32 := func(t parquet.Type) bool {
		if t.Kind() != parquet.Int32 {
			return false
		}
		logical := t.LogicalType()
		if logical == nil {
			return true
		}
		integer, ok := logical.Value.(*format.IntType)
		return ok && integer.BitWidth == 32 && integer.IsSigned
	}
	return signedInt32(got) && signedInt32(want)
}

func (r *ParquetReader) Next(ctx context.Context) (domain.TelemetryRecord, error) {
	if err := ctx.Err(); err != nil {
		return domain.TelemetryRecord{}, err
	}
	if r.index == r.count {
		if r.pending != nil {
			return domain.TelemetryRecord{}, r.pending
		}
		r.count, r.pending = r.reader.Read(r.buffer)
		r.index = 0
		if r.count == 0 {
			if r.pending == nil {
				return domain.TelemetryRecord{}, io.ErrNoProgress
			}
			return domain.TelemetryRecord{}, r.pending
		}
	}
	row := r.buffer[r.index]
	r.index++
	record, err := row.record()
	if err != nil {
		return domain.TelemetryRecord{}, err
	}
	day := record.Timestamp.Format("2006-01-02")
	if r.firstDay == "" {
		r.firstDay = day
	}
	if day != r.firstDay || row.Timestamp%10_000_000 != 0 {
		return domain.TelemetryRecord{}, fmt.Errorf("prepared timestamps must be on the 10-second grid in one UTC day")
	}
	return record, nil
}

func (r *ParquetReader) Close() error {
	return errors.Join(r.reader.Close(), r.file.Close())
}

func (row preparedRow) record() (domain.TelemetryRecord, error) {
	record := domain.TelemetryRecord{Timestamp: time.UnixMicro(row.Timestamp).UTC(), Hostname: row.Hostname, GroupID: row.GroupID, NodeIndex: int(row.NodeIndex), Valid: row.Valid, AvailabilityMask: row.AvailabilityMask}
	pointers := [domain.FeatureCount]*float64{row.PSU0Power, row.PSU1Power, row.CPU0Power, row.CPU1Power, row.GPU0Power, row.GPU1Power, row.GPU2Power, row.GPU3Power, row.GPU4Power, row.GPU5Power, row.CPU0Temp, row.CPU1Temp, row.GPU0Temp, row.GPU1Temp, row.GPU2Temp, row.GPU3Temp, row.GPU4Temp, row.GPU5Temp}
	var values [domain.FeatureCount]float64
	for i, p := range pointers {
		if p == nil {
			values[i] = math.NaN()
		} else {
			values[i] = *p
		}
	}
	record.SetFeatures(values)
	if err := record.Validate(); err != nil {
		return domain.TelemetryRecord{}, err
	}
	for i, count := range []int32{row.CPU0ValidCores, row.CPU1ValidCores} {
		// -1 means the source supplied a mean but no per-core availability.
		if count < -1 || (count != -1 && (count > 0) != (row.AvailabilityMask&(1<<uint(10+i)) != 0)) {
			return domain.TelemetryRecord{}, fmt.Errorf("invalid CPU%d core availability metadata", i)
		}
	}
	return record, nil
}
