package core_test

import (
	"kv/core"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

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

func randomKey(minSize int, maxSize int) string {
	return randomString(rand.Intn(maxSize-minSize) + minSize)
}

func benchmarkSet(valueSize int, engine *core.Engine, b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := randomKey(100, 200)
		value := randomString(valueSize)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(valueSize int, engine *core.Engine, b *testing.B) {
	b.ReportAllocs()

	key := randomKey(100, 200)
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
		SegmentMaxSize:             1000,
		SnapshotInterval:           5 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          15 * time.Second,
		CompactorWorkerCount:       10,
		SnapshotTTLDuration:        10 * time.Second,
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

const WorkersCount = 2

func TestConcurrentWrites(t *testing.T) {
	engine, err := makeEngine(t)

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	wg := new(sync.WaitGroup)
	for z := 0; z < WorkersCount; z++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < 10000; i++ {
				log.Printf("worker %v - job %v", id, i)
				if err := engine.Set(randomKey(200, 400), randomString(500)); err != nil {
					panic(err)
				}
				if err := engine.Set(randomKey(400, 600), randomString(1000)); err != nil {
					panic(err)
				}
				if err := engine.Set(randomKey(600, 800), randomString(1500)); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(wg, z)
		wg.Add(1)
	}

	wg.Wait()
}

func TestConcurrentReads(t *testing.T) {
	engine, err := makeEngine(t)

	if err != nil {
		t.Fatal(err)
	}

	defer engine.Close()

	expectedKey := randomKey(200, 400)
	if err := engine.Set(expectedKey, randomString(1000)); err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	for z := 0; z < WorkersCount; z++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < 10000; i++ {
				log.Printf("worker %v - job %v", id, i)
				if _, err := engine.Get(expectedKey); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(wg, z)
		wg.Add(1)
	}

	wg.Wait()
}
