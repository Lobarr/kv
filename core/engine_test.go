package core_test

import (
	"fmt"
	"kv/core"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
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

func benchmarkSet(valueSize int, engine *core.Engine, b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := randomStringBetween(100, 200)
		value := randomString(valueSize)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(valueSize int, engine *core.Engine, b *testing.B) {
	b.ReportAllocs()

	key := randomStringBetween(100, 200)
	value := randomString(valueSize)

	if err := engine.Set(key, value); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := engine.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func makeEngine(t testing.TB) (*core.Engine, error) {
	return core.NewEngine(&core.EngineConfig{
		SegmentMaxSize:             100,
		SnapshotInterval:           5 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          10 * time.Second,
		CompactorWorkerCount:       10,
		SnapshotTTLDuration:        10 * time.Second,
		DataPath:                   t.TempDir(),
	})
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

const ReadsWorkersCount = 100
const ReadsJobsCount = 10000
const WritesWorkersCount = 10
const WritesJobsCount = 500

func TestConcurrentWrites(t *testing.T) {
	start := time.Now()
	engine, err := makeEngine(t)
	jobCount := WritesWorkersCount * WritesJobsCount
	bar := progressbar.Default(int64(jobCount))

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	wg := new(sync.WaitGroup)
	keysLengthWritten := 0
	valuesLengthWritten := 0

	for z := 0; z < WritesWorkersCount; z++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < WritesJobsCount; i++ {
				key := randomStringBetween(600, 800)
				value := randomStringBetween(1500, 3000)

				keysLengthWritten += len(key)
				valuesLengthWritten += len(value)

				// logrus.Printf("concurrent writes worker %d - job %d - key size %d - value size %d", id, i, len(key), len(value))

				if err := engine.Set(key, value); err != nil {
					panic(err)
				}

				if os.Getenv("CI") != "true" {
					bar.Add(1)
				}
			}
			wg.Done()
		}(wg, z)
		wg.Add(1)
	}

	wg.Wait()

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
		expectedKey := randomStringBetween(200, 400)
		keys[i] = expectedKey

		if err := engine.Set(expectedKey, randomString(1000)); err != nil {
			t.Fatal(err)
		}
	}

	wg := new(sync.WaitGroup)
	for z := 0; z < ReadsWorkersCount; z++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < ReadsJobsCount; i++ {
				keyIndex := rand.Intn(keysLength)
				key := keys[keyIndex]

				// logrus.Printf("concurrent reads worker %d - job %d - key size %d - key index %d", id, i, len(key), keyIndex)

				if _, err := engine.Get(key); err != nil {
					panic(fmt.Sprintf("%v: key - %s", err, key))
				}

				if os.Getenv("CI") != "true" {
					bar.Add(1)
				}
			}
			wg.Done()
		}(wg, z)
		wg.Add(1)
	}

	wg.Wait()

	duration := time.Since(start).Seconds()
	rate := float64(jobCount) / duration
	logrus.Printf(
		"total reads %d - %f reads/s - duration %fs",
		jobCount, rate, duration,
	)
}
