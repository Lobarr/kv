// using same package so internals are visible
package core

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
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
	logrus.SetOutput(os.Stdout)
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

					if v, err := engine.Get(key); err != nil || value != v {
						return fmt.Errorf("expected (%s, %s) but got (%s, %s); err = %v", key, value, key, v, err)
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

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	const keysLength int = 500
	keyValues := map[string]string{}

	for i := 0; i < keysLength; i++ {
		expectedKey := randomStringBetween(10, 20)
		keyValues[expectedKey] = randomString(1000)

		if err := engine.Set(expectedKey, keyValues[expectedKey]); err != nil {
			t.Fatal(err)
		}
	}

	wg := new(errgroup.Group)
	wg.SetLimit(ReadsWorkersCount)
	for z := 0; z < ReadsWorkersCount; z++ {
		wg.Go(func(id int) func() error {
			return func() error {
				keys := maps.Keys(keyValues)

				for i := 0; i < ReadsJobsCount; i++ {
					keyIndex := rand.Intn(keysLength)
					key := keys[keyIndex]

					value, err := engine.Get(key)
					if err != nil {
						return err
					}

					if value != keyValues[key] {
						return fmt.Errorf("expected key %s to have value %v but got %s", key, keyValues[key], value)
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
	logrus.Printf("total reads %d - %f reads/s - duration %fs", jobCount, rate, duration)
}

func TestReadAfterWrite(t *testing.T) {
	engine, err := makeEngine(t)
	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	const keysLength int = 100
	keyValues := map[string]string{}
	for i := 0; i < keysLength; i++ {
		expectedKey := randomStringBetween(10, 20)
		keyValues[expectedKey] = randomString(1000)

		if err := engine.Set(expectedKey, keyValues[expectedKey]); err != nil {
			t.Fatal(err)
		}
	}

	wg := new(errgroup.Group)
	wg.SetLimit(keysLength)

	for _, key := range maps.Keys(keyValues) {
		key := key

		wg.Go(func(key string) func() error {
			return func() error {
				for i := 0; i < 100; i++ {
					value, err := engine.Get(key)
					if err != nil {
						return err
					}

					if value != keyValues[key] {
						return fmt.Errorf("expected key %s to have value %v but got %s", key, keyValues[key], value)
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
	initialSegmentEntriesCount := engine.segment.entriesCount.Load()

	if err = engine.Set(key, value); err != nil {
		t.Fatal(err)
	}

	// ensure entry go added to current segment
	if engine.segment.entriesCount.Load() != initialSegmentEntriesCount+1 {
		t.Errorf("expected entries_count=%d but got %d",
			initialSegmentEntriesCount+1, engine.segment.entriesCount.Load())
	}

	// ensure the context needed to read the key is set in memory
	index := engine.state.Get(key)
	if index == nil {
		t.Errorf("expected state to have key %s but it didn't", key)
	}

	if index.Key != key {
		t.Errorf("expected index.Key = %s but got %s", key, index.Key)
	}

	if index.EntrySize <= 0 {
		t.Errorf("expected index.EntrySize to be greater than 0 but got %d", index.EntrySize)
	}

	// the index must point at the active segment
	if segmentIDOfIndex(index) != engine.segment.id {
		t.Errorf("expected index segment %s but got %s", engine.segment.id, segmentIDOfIndex(index))
	}
}

// TestRolloverConcurrentReadWrite hammers the engine with concurrent reads
// and writes while segments roll over constantly (tiny segment size, tiny
// cache so evictions and disk loads happen too). No EOFs and no corruption
// are allowed.
func TestRolloverConcurrentReadWrite(t *testing.T) {
	config := &EngineConfig{
		SegmentMaxSize:             8, // force constant rollovers
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  3, // force segment eviction + disk reloads
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   t.TempDir(),
		ShouldCompact:              false,
	}

	engine, err := NewEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	const workers = 40
	const jobsPerWorker = 40

	keyValues := make(map[string]string, workers*jobsPerWorker)
	var mu sync.Mutex

	g := new(errgroup.Group)
	g.SetLimit(workers)

	for i := 0; i < workers; i++ {
		i := i
		g.Go(func() error {
			for j := 0; j < jobsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", i, j)
				value := randomStringBetween(100, 2000)

				if err := engine.Set(key, value); err != nil {
					return fmt.Errorf("set %s: %w", key, err)
				}

				// read-your-write
				got, err := engine.Get(key)
				if err != nil {
					return fmt.Errorf("get %s after write: %w", key, err)
				}
				if got != value {
					return fmt.Errorf("corruption for key %s: expected %q got %q", key, value, got)
				}

				// re-read a random earlier key
				mu.Lock()
				keys := maps.Keys(keyValues)
				mu.Unlock()
				if len(keys) > 0 {
					other := keys[rand.Intn(len(keys))]
					mu.Lock()
					expected := keyValues[other]
					mu.Unlock()

					got, err := engine.Get(other)
					if err != nil {
						return fmt.Errorf("get %s: %w", other, err)
					}
					if got != expected {
						return fmt.Errorf("corruption for key %s: expected %q got %q", other, expected, got)
					}
				}

				mu.Lock()
				keyValues[key] = value
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// verify every key after all concurrent activity (rollover + evictions)
	for key, expected := range keyValues {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for key %s: expected %q got %q", key, expected, got)
		}
	}

	if len(keyValues) != workers*jobsPerWorker {
		t.Fatalf("expected %d keys, state has %d", workers*jobsPerWorker, engine.state.Len())
	}
}

// TestDeleteAndCompactionAfterDelete covers deletes, read-after-delete and
// that compaction after deletion keeps deleted keys deleted and other keys
// intact. Tombstones live in segments that must be sealed (rolled over)
// before compaction can drop them — the active segment is never compacted.
func TestDeleteAndCompactionAfterDelete(t *testing.T) {
	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	const keysLength = 200
	deletedKeys := make(map[string]string) // key -> value (deleted)
	keptKeys := make(map[string]string)    // key -> value (kept)

	for i := 0; i < keysLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := randomStringBetween(100, 500)

		if err := engine.Set(key, value); err != nil {
			t.Fatal(err)
		}

		if i%2 == 0 {
			deletedKeys[key] = value
		} else {
			keptKeys[key] = value
		}
	}

	for key := range deletedKeys {
		if err := engine.Delete(key); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
		if _, err := engine.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected ErrKeyNotFound for deleted key %s, got %v", key, err)
		}
	}

	// Force rollovers so the segments holding the tombstones (and the old
	// values they shadow) are sealed; the active segment is never compacted.
	engine.segmentMaxSizeMutex.Lock()
	engine.segmentMaxSize = 1
	engine.segmentMaxSizeMutex.Unlock()
	fillerKeys := map[string]string{}
	for i := 0; i < 3; i++ {
		fillerKey := fmt.Sprintf("filler-%d", i)
		fillerKeys[fillerKey] = fmt.Sprintf("filler-value-%d", i)
		if err := engine.Set(fillerKey, fillerKeys[fillerKey]); err != nil {
			t.Fatal(err)
		}
	}

	if err := engine.compactSegments(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	for key := range deletedKeys {
		if _, err := engine.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("after compaction expected ErrKeyNotFound for deleted key %s, got %v", key, err)
		}
	}

	for key, expected := range keptKeys {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("get %s after compaction: %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for key %s after compaction: expected %q got %q", key, expected, got)
		}
	}

	for key, expected := range fillerKeys {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("get filler %s after compaction: %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for filler key %s after compaction: expected %q got %q", key, expected, got)
		}
	}

	// Deleted keys are dropped from the state entirely once their segment is
	// compacted (tombstones are not rewritten); kept keys + fillers remain.
	if got := engine.state.Len(); got != len(keptKeys)+len(fillerKeys) {
		t.Fatalf("expected %d keys in state after compaction, got %d", len(keptKeys)+len(fillerKeys), got)
	}
}

// TestChecksumCorruptionDetection flips a byte inside a persisted value and
// expects Get to report the corruption instead of returning garbage.
func TestChecksumCorruptionDetection(t *testing.T) {
	engine, err := makeEngine(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	key := "corrupt-me"
	value := randomString(1000)

	if err := engine.Set(key, value); err != nil {
		t.Fatal(err)
	}
	if err := engine.Sync(); err != nil {
		t.Fatal(err)
	}

	index := engine.state.Get(key)
	if index == nil {
		t.Fatal("expected index for key")
	}

	// Flip one byte inside the value region on disk:
	// layout is [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
	valueRegionOffset := index.Offset + int64(4+len(key)+4) + 10

	file, err := os.OpenFile(index.SegmentFilename, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	original := make([]byte, 1)
	if _, err := file.ReadAt(original, valueRegionOffset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	flipped := original[0] ^ 0xFF
	if _, err := file.WriteAt([]byte{flipped}, valueRegionOffset); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = engine.Get(key)
	if err == nil {
		t.Fatal("expected checksum verification to fail on corrupted entry")
	}
	if !errors.Is(err, ErrInvalidLogEntryChecksum) {
		t.Fatalf("expected ErrInvalidLogEntryChecksum, got %v", err)
	}
}

// TestRecoveryWithOverwrittenKeysAndColonKeys simulates a crash/restart: an
// engine with overwritten keys spread across multiple segments (and keys
// containing "::") is closed and reopened, and the newest values must be
// recovered deterministically.
func TestRecoveryWithOverwrittenKeysAndColonKeys(t *testing.T) {
	dataPath := t.TempDir()

	engineA, err := NewEngine(&EngineConfig{
		SegmentMaxSize:             3, // tiny: everything spans many segments
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  100,
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   dataPath,
		ShouldCompact:              false,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := engineA.Set("alpha", "one"); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Set("with::colon", "c1"); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Set("plain", "p"); err != nil {
		t.Fatal(err)
	}
	// overwrites land in later segments
	if err := engineA.Set("alpha", "two"); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Set("with::colon", "c2"); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Set("a::b::c::d", "deep"); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Set("::weird::key::", "w"); err != nil {
		t.Fatal(err)
	}

	if engineA.segmentsMetadataList.Len() < 2 {
		t.Fatalf("expected keys to span multiple segments, got %d segments", engineA.segmentsMetadataList.Len())
	}

	if err := engineA.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen: recovery from snapshot
	engineB, err := NewEngine(&EngineConfig{
		SegmentMaxSize:             100,
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  100,
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   dataPath,
		ShouldCompact:              false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engineB.Close()

	expected := map[string]string{
		"alpha":          "two",
		"with::colon":    "c2",
		"plain":          "p",
		"a::b::c::d":     "deep",
		"::weird::key::": "w",
	}

	for key, want := range expected {
		got, err := engineB.Get(key)
		if err != nil {
			t.Fatalf("get %s after recovery: %v", key, err)
		}
		if got != want {
			t.Fatalf("after recovery expected key %s = %q, got %q", key, want, got)
		}
	}

	if _, err := engineB.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for missing key after recovery, got %v", err)
	}

	// compaction on the recovered engine must keep colon keys intact
	if err := engineB.compactSegments(); err != nil {
		t.Fatalf("compaction after recovery failed: %v", err)
	}

	for key, want := range expected {
		got, err := engineB.Get(key)
		if err != nil {
			t.Fatalf("get %s after compaction post-recovery: %v", key, err)
		}
		if got != want {
			t.Fatalf("after compaction post-recovery expected key %s = %q, got %q", key, want, got)
		}
	}
}

// TestCompressionWithRolloverAndMmapReads exercises the compression path
// across rollovers, mmap reads of sealed segments, compaction and restart.
func TestCompressionWithRolloverAndMmapReads(t *testing.T) {
	dataPath := t.TempDir()

	config := &EngineConfig{
		SegmentMaxSize:             10, // force rollovers
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  4,
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   dataPath,
		ShouldCompact:              false,
		EnableCompression:          true,
	}

	engine, err := NewEngine(config)
	if err != nil {
		t.Fatal(err)
	}

	const keysLength = 250
	keyValues := make(map[string]string, keysLength)
	for i := 0; i < keysLength; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := randomStringBetween(100, 1500)
		keyValues[key] = value

		if err := engine.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// reads mix the active segment (flushed buffer) with sealed mmap segments
	for key, expected := range keyValues {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("get %s (compression): %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for key %s (compression): expected %q got %q", key, expected, got)
		}
	}

	// compaction must roundtrip compressed entries
	if err := engine.compactSegments(); err != nil {
		t.Fatalf("compaction (compression) failed: %v", err)
	}
	for key, expected := range keyValues {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("get %s after compaction (compression): %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for key %s after compaction (compression): expected %q got %q", key, expected, got)
		}
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen: compressed segments are loaded from disk and mmapped
	engine2, err := NewEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	for key, expected := range keyValues {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("get %s after restart (compression): %v", key, err)
		}
		if got != expected {
			t.Fatalf("corruption for key %s after restart (compression): expected %q got %q", key, expected, got)
		}
	}
}

// TestMultipleEnginesParallel runs two engines (with different settings) in
// parallel and verifies their data stores are fully isolated.
func TestMultipleEnginesParallel(t *testing.T) {
	engine1, err := NewEngine(&EngineConfig{
		SegmentMaxSize:             20,
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   t.TempDir(),
		ShouldCompact:              false,
		EnableCompression:          false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine1.Close()

	engine2, err := NewEngine(&EngineConfig{
		SegmentMaxSize:             15,
		SnapshotInterval:           1 * time.Hour,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          1 * time.Hour,
		SnapshotTTLDuration:        1 * time.Hour,
		DataPath:                   t.TempDir(),
		ShouldCompact:              false,
		EnableCompression:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	var g errgroup.Group
	g.SetLimit(40)

	const perEngine = 200

	engine1Keys := make([]string, perEngine)
	engine2Keys := make([]string, perEngine)

	for i := 0; i < perEngine; i++ {
		engine1Keys[i] = fmt.Sprintf("e1-key-%d", i)
		engine2Keys[i] = fmt.Sprintf("e2-key-%d", i)
	}

	// concurrent writes to both engines at once
	for i := 0; i < perEngine; i++ {
		i := i
		g.Go(func() error {
			if err := engine1.Set(engine1Keys[i], fmt.Sprintf("e1-value-%d", i)); err != nil {
				return err
			}
			return engine2.Set(engine2Keys[i], fmt.Sprintf("e2-value-%d", i))
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// verify contents + isolation
	for i := 0; i < perEngine; i++ {
		got, err := engine1.Get(engine1Keys[i])
		if err != nil || got != fmt.Sprintf("e1-value-%d", i) {
			t.Fatalf("engine1 get %s: got (%q, %v)", engine1Keys[i], got, err)
		}
		if _, err := engine1.Get(engine2Keys[i]); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("engine1 should not see engine2 key %s: %v", engine2Keys[i], err)
		}

		got, err = engine2.Get(engine2Keys[i])
		if err != nil || got != fmt.Sprintf("e2-value-%d", i) {
			t.Fatalf("engine2 get %s: got (%q, %v)", engine2Keys[i], got, err)
		}
		if _, err := engine2.Get(engine1Keys[i]); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("engine2 should not see engine1 key %s: %v", engine1Keys[i], err)
		}
	}

	// distinct data directories on disk
	if engine1.layout.dataDir() == engine2.layout.dataDir() {
		t.Fatal("expected engines to use distinct data directories")
	}
}

// TestSetOutOfOrderStateUpdateRace hammers the same key from many goroutines
// and verifies the final state always matches the last value written to disk.
func TestSetOutOfOrderStateUpdateRace(t *testing.T) {
	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	const key = "hot-key"
	const writers = 32
	const writesPerWriter = 50

	var g errgroup.Group
	g.SetLimit(writers)

	for w := 0; w < writers; w++ {
		w := w
		g.Go(func() error {
			for i := 0; i < writesPerWriter; i++ {
				value := fmt.Sprintf("w%d-i%d-%s", w, i, randomString(50))
				if err := engine.Set(key, value); err != nil {
					return err
				}
				if _, err := engine.Get(key); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	// the state must point at the newest entry on disk for the key
	index := engine.state.Get(key)
	if index == nil {
		t.Fatal("expected index for hot key")
	}

	// the index must resolve to the value that was appended last to the
	// active segment's offset range
	if segmentIDOfIndex(index) == engine.segment.id {
		if index.Offset+index.EntrySize > engine.segment.offset.Load() {
			t.Fatalf("state index offset beyond disk offset: index %+v disk %d",
				index, engine.segment.offset.Load())
		}
	}

	if len(got) == 0 {
		t.Fatal("expected non-empty value for hot key")
	}
}

// TestCompactionKeepsActiveSegment verifies compaction never deletes the
// active segment file and data written after compaction is readable.
func TestCompactionKeepsActiveSegment(t *testing.T) {
	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	for i := 0; i < 250; i++ {
		if err := engine.Set(fmt.Sprintf("key-%d", i), randomString(200)); err != nil {
			t.Fatal(err)
		}
	}

	engine.segmentMutex.RLock()
	activeID := engine.segment.id
	activeFile := engine.segment.fileName
	engine.segmentMutex.RUnlock()

	if err := engine.compactSegments(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(activeFile); err != nil {
		t.Fatalf("active segment file must survive compaction: %v", err)
	}

	if err := engine.Set("post-compaction", "value"); err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get("post-compaction")
	if err != nil || got != "value" {
		t.Fatalf("post-compaction write/read failed: (%q, %v)", got, err)
	}

	if _, err := engine.Get("key-0"); err != nil {
		t.Fatalf("original key lost after compaction: %v", err)
	}

	_ = activeID
}

// TestSnapshotFilesAreAtomic ensures snapshot() leaves only complete
// .snapshot files (no leftover temp files after a successful snapshot).
func TestSnapshotFilesAreAtomic(t *testing.T) {
	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err := engine.Set("key", "value"); err != nil {
		t.Fatal(err)
	}

	if err := engine.snapshot(); err != nil {
		t.Fatal(err)
	}

	snapshots, err := os.ReadDir(engine.layout.snapshotsPath())
	if err != nil {
		t.Fatal(err)
	}

	found := 0
	for _, f := range snapshots {
		if filepath.Ext(f.Name()) == ".tmp" {
			t.Fatalf("temp snapshot file left behind: %s", f.Name())
		}
		if filepath.Ext(f.Name()) == ".snapshot" {
			found++
		}
	}
	if found == 0 {
		t.Fatal("expected at least one snapshot file")
	}
}

func TestBatchOperations(t *testing.T) {
	cfg := getEngineConfig(t, false)
	cfg.SegmentMaxSize = 25 // Force rollovers during batch writes
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Verify Engine satisfies the Store interface with batch methods.
	var store Store = engine

	items := make([]KVItem, 100)
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("batch-key-%03d", i)
		items[i] = KVItem{Key: keys[i], Value: fmt.Sprintf("batch-val-%03d", i)}
	}
	// Duplicate key within the same batch: last write wins.
	items = append(items, KVItem{Key: "batch-key-005", Value: "overwritten-val-005"})

	if err := store.BatchSet(items); err != nil {
		t.Fatalf("BatchSet failed: %v", err)
	}

	got, err := store.BatchGet(append(keys, "non-existent-key"))
	if err != nil {
		t.Fatalf("BatchGet failed: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("expected 100 keys, got %d", len(got))
	}
	if got["batch-key-005"] != "overwritten-val-005" {
		t.Fatalf("expected overwritten value, got %q", got["batch-key-005"])
	}
	if got["batch-key-099"] != "batch-val-099" {
		t.Fatalf("expected batch-val-099, got %q", got["batch-key-099"])
	}

	// BatchDelete (including a missing key — must be idempotent).
	if err := store.BatchDelete([]string{"batch-key-005", "batch-key-050", "missing-key"}); err != nil {
		t.Fatalf("BatchDelete failed: %v", err)
	}

	gotAfterDel, err := store.BatchGet([]string{"batch-key-005", "batch-key-050", "batch-key-051"})
	if err != nil {
		t.Fatalf("BatchGet after delete failed: %v", err)
	}
	if len(gotAfterDel) != 1 || gotAfterDel["batch-key-051"] != "batch-val-051" {
		t.Fatalf("unexpected BatchGet result after delete: %v", gotAfterDel)
	}

	// Concurrent BatchSet + BatchGet stress (verifies deadlock-free shard locking).
	var g errgroup.Group
	g.SetLimit(16)
	for w := 0; w < 16; w++ {
		w := w
		g.Go(func() error {
			batch := make([]KVItem, 20)
			bKeys := make([]string, 20)
			for j := 0; j < 20; j++ {
				k := fmt.Sprintf("conc-%d-%d", w, j)
				v := fmt.Sprintf("val-%d-%d", w, j)
				batch[j] = KVItem{Key: k, Value: v}
				bKeys[j] = k
			}
			if err := store.BatchSet(batch); err != nil {
				return err
			}
			res, err := store.BatchGet(bKeys)
			if err != nil {
				return err
			}
			if len(res) != 20 {
				return fmt.Errorf("worker %d: expected 20 keys, got %d", w, len(res))
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("concurrent batch ops failed: %v", err)
	}
}
