package parquet

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/parquet-go/parquet-go"
)

func parquetFixtureRow() preparedRow {
	row := preparedRow{Timestamp: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro(), Hostname: "a01n01", GroupID: "a01", NodeIndex: 1, Valid: true, AvailabilityMask: domain.FullAvailabilityMask, CPU0ValidCores: 23, CPU1ValidCores: 23}
	v := reflect.ValueOf(&row).Elem()
	for i := 0; i < 18; i++ {
		value := float64(i + 1)
		v.Field(i + 4).Set(reflect.ValueOf(&value))
	}
	return row
}

func writeFixture(t *testing.T, row preparedRow, withVersion bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic.parquet")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	options := []parquet.WriterOption{}
	if withVersion {
		options = append(options, parquet.KeyValueMetadata("schema_version", PreparedSchemaVersion))
	}
	writer := parquet.NewGenericWriter[preparedRow](file, options...)
	if _, err := writer.Write([]preparedRow{row}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParquetReaderPreservesF18AndMissingness(t *testing.T) {
	row := parquetFixtureRow()
	row.PSU0Power = nil
	row.Valid = false
	row.AvailabilityMask &^= 1
	reader, err := OpenParquet(writeFixture(t, row, true))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	record, err := reader.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if record.Timestamp.UnixMicro() != row.Timestamp || record.Timestamp.Location() != time.UTC || reader.Rows() != 1 {
		t.Fatal("logical timestamp/rows changed")
	}
	if !math.IsNaN(record.PSUPower[0]) || record.PSUPower[1] != 2 || record.CPUPower != [2]float64{3, 4} || record.GPUPower != [6]float64{5, 6, 7, 8, 9, 10} || record.CPUTemp != [2]float64{11, 12} || record.GPUTemp != [6]float64{13, 14, 15, 16, 17, 18} {
		t.Fatalf("wrong F18: %+v", record)
	}
	if record.Valid || record.AvailabilityMask != row.AvailabilityMask {
		t.Fatal("missingness lost")
	}
	if _, err := reader.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestParquetRejectsMetadataInvalidStateAndCorruptFiles(t *testing.T) {
	if _, err := OpenParquet(writeFixture(t, parquetFixtureRow(), false)); err == nil {
		t.Fatal("missing schema version accepted")
	}
	for _, modify := range []func(*preparedRow){
		func(r *preparedRow) { r.PSU0Power = nil },
		func(r *preparedRow) { r.CPU0ValidCores = 0 },
		func(r *preparedRow) { r.Timestamp++ },
		func(r *preparedRow) { r.Hostname = "a01n19" },
	} {
		row := parquetFixtureRow()
		modify(&row)
		reader, err := OpenParquet(writeFixture(t, row, true))
		if err != nil {
			t.Fatal(err)
		}
		_, err = reader.Next(context.Background())
		_ = reader.Close()
		if err == nil {
			t.Fatal("invalid prepared record accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "broken.parquet")
	if err := os.WriteFile(path, []byte("PAR1broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenParquet(path); err == nil {
		t.Fatal("truncated parquet accepted")
	}
	if _, err := OpenParquet(path + ".missing"); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestSourceProvidedCPUMeanUnknownCoreCount(t *testing.T) {
	row := parquetFixtureRow()
	row.CPU0ValidCores, row.CPU1ValidCores = -1, -1
	if _, err := row.record(); err != nil {
		t.Fatal(err)
	}
	row.CPU0ValidCores = -2
	if _, err := row.record(); err == nil {
		t.Fatal("invalid sentinel accepted")
	}
}

func TestParquetSchemaContract(t *testing.T) {
	for name, modify := range map[string]func(parquet.Group){
		"required feature":  func(g parquet.Group) { g["psu0_power"] = parquet.Required(parquet.Leaf(parquet.DoubleType)) },
		"float feature":     func(g parquet.Group) { g["psu0_power"] = parquet.Optional(parquet.Leaf(parquet.FloatType)) },
		"timestamp unit":    func(g parquet.Group) { g["timestamp"] = parquet.Leaf(parquet.Int64Type) },
		"nullable identity": func(g parquet.Group) { g["hostname"] = parquet.Optional(parquet.String()) },
		"missing":           func(g parquet.Group) { delete(g, "gpu5_core_temp") },
		"extra":             func(g parquet.Group) { g["extra"] = parquet.String() },
	} {
		t.Run(name, func(t *testing.T) {
			fields := parquet.Group{}
			for _, field := range parquet.SchemaOf(preparedRow{}).Fields() {
				fields[field.Name()] = field
			}
			modify(fields)
			path := filepath.Join(t.TempDir(), "bad-schema.parquet")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w := parquet.NewWriter(f, parquet.NewSchema("preparedRow", fields), parquet.KeyValueMetadata("schema_version", PreparedSchemaVersion))
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if r, err := OpenParquet(path); err == nil {
				r.Close()
				t.Fatal("incompatible schema accepted")
			} else if !strings.Contains(err.Error(), "column") {
				t.Fatalf("schema mismatch was not detected before reading: %v", err)
			}
		})
	}
}

func TestParquetContextDayGridAndEmpty(t *testing.T) {
	path := writeFixture(t, parquetFixtureRow(), true)
	r, err := OpenParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]preparedRow{
		nil,
		{func() preparedRow { r := parquetFixtureRow(); r.Timestamp = time.Time{}.UnixMicro(); return r }()},
		{parquetFixtureRow(), func() preparedRow {
			r := parquetFixtureRow()
			r.Timestamp += int64(24 * time.Hour / time.Microsecond)
			return r
		}()},
	} {
		path := filepath.Join(t.TempDir(), "time.parquet")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		w := parquet.NewGenericWriter[preparedRow](f, parquet.KeyValueMetadata("schema_version", PreparedSchemaVersion))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := OpenParquet(path)
		if len(rows) == 0 {
			if err == nil {
				r.Close()
				t.Fatal("empty file accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for range rows {
			_, err = r.Next(context.Background())
			if err != nil {
				break
			}
		}
		r.Close()
		if err == nil {
			t.Fatal("invalid time accepted")
		}
	}
}
