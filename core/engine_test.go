package core_test

import (
	"kv/core"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
	//log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func benchmarkSet(valueSize int, engine *core.Engine, b *testing.B) {
	for i := 0; i < b.N; i++ {
		key := randomString(rand.Intn(30))
		value := randomString(valueSize)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(valueSize int, engine *core.Engine, b *testing.B) {
	key := randomString(rand.Intn(30))
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
		CacheSize:                  5,
		CompactorInterval:          3 * time.Second,
		CompactorWorkerCount:       3,
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
}

func TestConcurrentWrites(t *testing.T) {
	engine, err := makeEngine(t)
	defer engine.Close()

	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 50; i++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < 100; i++ {
				if err := engine.Set("key", "some-value"); err != nil {
					panic(err)
				}
				if err := engine.Set("some-key", "new-value"); err != nil {
					panic(err)
				}
				if err := engine.Set("json", "{'ping': 'pong'}"); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(wg, i)
		wg.Add(1)
	}

	wg.Wait()
}
