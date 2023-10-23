// using same package so internals are visible
package core

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	ReadsWorkersCount  = 1000
	ReadsJobsCount     = 50
	WritesWorkersCount = 100
	WritesJobsCount    = 25
)

func init() {
	logrus.SetLevel(logrus.InfoLevel)
	if os.Getenv("CI") != "true" {
		logrus.SetOutput(os.Stdout)
	} else {
		logrus.SetOutput(ioutil.Discard)
	}
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func randomStringBetween(minSize int, maxSize int) string {
	return randomString(rand.Intn(maxSize-minSize) + minSize)
}

func benchmarkSet(valueSize int, engine *Engine, b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := randomStringBetween(10, 20)
		value := randomString(valueSize)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(valueSize int, engine *Engine, b *testing.B) {
	b.ReportAllocs()

	key := randomStringBetween(10, 20)
	value := randomString(valueSize)

	if err := engine.Set(key, value); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := engine.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func getEngineConfig(t testing.TB, shouldCompact bool) *EngineConfig {
	return &EngineConfig{
		SegmentMaxSize:             100,
		SnapshotInterval:           10 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  1000,
		CompactorInterval:          10 * time.Second,
		CompactorWorkerCount:       10,
		SnapshotTTLDuration:        10 * time.Second,
		DataPath:                   t.TempDir(),
		ShouldCompact:              shouldCompact,
	}
}

func makeEngine(t testing.TB) (*Engine, error) {
	return NewEngine(getEngineConfig(t, true))
}

func makeEngineWithoutCompaction(t testing.TB) (*Engine, error) {
	return NewEngine(getEngineConfig(t, false))
}

func BenchmarkSet(b *testing.B) {
	engine, err := makeEngine(b)

	if err != nil {
		b.Fatal(err)
	}

	defer engine.Close()

	b.Run("50", func(b *testing.B) {
		benchmarkSet(50, engine, b)
	})
	b.Run("500", func(b *testing.B) {
		benchmarkSet(500, engine, b)
	})
	b.Run("1000", func(b *testing.B) {
		benchmarkSet(1000, engine, b)
	})
	b.Run("10000", func(b *testing.B) {
		benchmarkSet(10000, engine, b)
	})
	b.Run("100000", func(b *testing.B) {
		benchmarkSet(100000, engine, b)
	})
	b.Run("1000000", func(b *testing.B) {
		benchmarkSet(1000000, engine, b)
	})
}

func BenchmarkGet(b *testing.B) {
	engine, err := makeEngine(b)

	if err != nil {
		b.Fatal(err)
	}

	defer engine.Close()

	b.Run("50", func(b *testing.B) {
		benchmarkGet(50, engine, b)
	})
	b.Run("500", func(b *testing.B) {
		benchmarkGet(500, engine, b)
	})
	b.Run("1000", func(b *testing.B) {
		benchmarkGet(1000, engine, b)
	})
	b.Run("10000", func(b *testing.B) {
		benchmarkGet(10000, engine, b)
	})
	b.Run("100000", func(b *testing.B) {
		benchmarkGet(100000, engine, b)
	})
	b.Run("1000000", func(b *testing.B) {
		benchmarkGet(1000000, engine, b)
	})
}

func TestConcurrentWrites(t *testing.T) {
	start := time.Now()
	engine, err := makeEngine(t)
	jobCount := WritesWorkersCount * WritesJobsCount
	bar := progressbar.Default(int64(jobCount))

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	writesGroup := new(errgroup.Group)
	writesGroup.SetLimit(WritesWorkersCount)

	keysLengthWritten := 0
	valuesLengthWritten := 0

	for z := 0; z < WritesWorkersCount; z++ {
		for i := 0; i < WritesJobsCount; i++ {
			key := randomStringBetween(10, 20)
			value := randomStringBetween(1500, 3000)

			keysLengthWritten += len(key)
			valuesLengthWritten += len(value)

			writesGroup.Go(func(key, value string) func() error {
				return func() error {
					if err := engine.Set(key, value); err != nil {
						return err
					}

					if os.Getenv("CI") != "true" {
						bar.Add(1)
					}

					return nil
				}
			}(key, value))
		}
	}

	if err := writesGroup.Wait(); err != nil {
		t.Fatal(err)
	}

	duration := time.Since(start).Seconds()
	rate := float64(jobCount) / duration
	logrus.Printf(
		"total writes %d - %f writes/s - duration %fs - keys written %s - values written %s",
		jobCount, rate, duration, humanize.Bytes(uint64(keysLengthWritten)), humanize.Bytes(uint64(valuesLengthWritten)),
	)
}

func TestConcurrentReads(t *testing.T) {
	start := time.Now()
	engine, err := makeEngine(t)
	jobCount := ReadsWorkersCount * ReadsJobsCount
	bar := progressbar.Default(int64(jobCount))

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	const keysLength int = 500
	keys := make([]string, keysLength)

	for i := 0; i < keysLength; i++ {
		expectedKey := randomStringBetween(10, 20)
		keys[i] = expectedKey

		if err := engine.Set(expectedKey, randomString(1000)); err != nil {
			t.Fatal(err)
		}
	}

	wg := new(errgroup.Group)
	wg.SetLimit(ReadsWorkersCount)
	for z := 0; z < ReadsWorkersCount; z++ {
		wg.Go(func(id int) func() error {
			return func() error {
				for i := 0; i < ReadsJobsCount; i++ {
					keyIndex := rand.Intn(keysLength)
					key := keys[keyIndex]

					if _, err := engine.Get(key); err != nil {
						return err
					}

					if os.Getenv("CI") != "true" {
						bar.Add(1)
					}
				}
				return nil
			}
		}(z))
	}

	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}

	duration := time.Since(start).Seconds()
	rate := float64(jobCount) / duration
	logrus.Printf(
		"total reads %d - %f reads/s - duration %fs",
		jobCount, rate, duration,
	)
}

func TestReadAfterWrite(t *testing.T) {
	engine, err := makeEngine(t)
	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	const keysLength int = 100
	keys := make([]string, keysLength)
	for i := 0; i < keysLength; i++ {
		expectedKey := randomStringBetween(10, 20)
		keys[i] = expectedKey

		if err := engine.Set(expectedKey, randomString(1000)); err != nil {
			t.Fatal(err)
		}
	}

	wg := new(errgroup.Group)
	wg.SetLimit(keysLength)

	for _, key := range keys {
		key := key

		wg.Go(func(key string) func() error {
			return func() error {
				for i := 0; i < 100; i++ {
					if _, err := engine.Get(key); err != nil {
						return err
					}
				}
				return nil
			}
		}(key))
	}

	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSet(t *testing.T) {
	engine, err := makeEngine(t)
	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	key := randomStringBetween(10, 20)
	value := randomString(1000)
	initialSegmentEntriesCount := engine.segment.entriesCount

	if err = engine.Set(key, value); err != nil {
		t.Fatal(err)
	}

	// ensure entry go added to current segment
	if engine.segment.entriesCount != initialSegmentEntriesCount+1 {
		t.Errorf("expected entries_count=%d but got %d",
			initialSegmentEntriesCount+1, engine.segment.entriesCount)
	}

	// ensure the context needed to read the key is set in memory
	indexesByKey, ok := engine.logEntryIndexesBySegmentID[engine.segment.id]
	if !ok {
		t.Errorf("expected logEntryIndexesBySegmentID to have key %s but it didn't", engine.segment.id)
	}

	index, ok := indexesByKey[key]
	if !ok {
		t.Errorf("expected indexesByKey to have key %s but it didn't", key)
	}

	if index.Key != key {
		t.Errorf("expected index.Key = %s but got %s", key, index.Key)
	}

	if index.EntrySize <= 0 || index.CompressedEntrySize <= 0 {
		t.Errorf("expected index.EntrySize and index.CompressedEntrySize to be greater than 0 but got (%d, %d) respectively",
			index.EntrySize, index.CompressedEntrySize)
	}
}

func TestSegmentCompaction(t *testing.T) {
	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	const keysLength int = 1000
	keys := make([]string, keysLength)
	values := make([]string, keysLength)
	for i := 0; i < keysLength; i++ {
		keys[i] = randomStringBetween(10, 20)
		values[i] = randomString(1000)

		if err := engine.Set(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}

	prevSegmentsCount := engine.segmentsMetadataList.Len()

	if err := engine.compactSegments(); err != nil {
		t.Fatal(err)
	}

	if engine.segmentsMetadataList.Len() >= prevSegmentsCount {
		t.Errorf("expected some segments to have been pruned in the compaction; had %d segments before compaction and %d segments after. found segments %v",
			prevSegmentsCount, engine.segmentsMetadataList.Len(), engine.segmentsMetadataList.GetSegmentIDs())
	}

	for i, key := range keys {
		value, err := engine.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		if value != values[i] {
			t.Errorf("expected value %v, got %v", values[i], value)
		}
	}
}
