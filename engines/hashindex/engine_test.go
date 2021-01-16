package hashindex

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func benchmarkSet(n int, engine *Engine, b *testing.B) {
	for i := 0; i < b.N; i++ {
		key := randomString(rand.Intn(30))
		value := randomString(n)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(n int, engine *Engine, b *testing.B) {
	for i := 0; i < b.N; i++ {
		value := randomString(n)
		key := randomString(rand.Intn(30))
		err := engine.Set(key, value)

		if err != nil {
			b.Fatal(err)
		}

		_, err = engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func makeEngine(t testing.TB) (*Engine, error) {
	return NewEngine(&EngineConfig{
		SegmentMaxSize:             100,
		SnapshotInterval:           2 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  3,
		CompactorInterval:          3 * time.Second,
		CompactorWorkerCount:       2,
		SnapshotTTLDuration:        5 * time.Second,
	})
}

func BenchmarkSet50(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(50, engine, b)
}

func BenchmarkSet500(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(500, engine, b)
}

func BenchmarkSet1000(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(1000, engine, b)
}

func BenchmarkGet50(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(50, engine, b)
}

func BenchmarkGet500(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(500, engine, b)
}

func BenchmarkGet1000(b *testing.B) {
	engine, err := makeEngine(b)
	defer engine.Close()

	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(1000, engine, b)
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
