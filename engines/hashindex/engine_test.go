package hashindex

import (
	"math/rand"
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
		key := randomString(n)
		value := randomString(n)

		if err := engine.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(n int, engine *Engine, b *testing.B) {
  value := randomString(n)
  key := randomString(n)
  err := engine.Set(key,value)
  
  if err != nil {
    b.Fatal(err)
  }

  for i := 0; i < b.N; i++ {
    _, err := engine.Get(key)
    if err != nil {
      b.Fatal(err)
    }
  }
}

var engine, err = NewEngine(&EngineConfig{
		SegmentMaxSize:             2000,
		SnapshotInterval:           2 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          5 * time.Second,
		CompactorWorkerCount:       3,
		SnapshotTTLDuration:        4 * time.Second,
})

func BenchmarkSet50(b *testing.B) {	
	if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(50, engine, b)
}

func BenchmarkSet500(b *testing.B) {
  if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(500, engine, b)
}

func BenchmarkSet5000(b *testing.B) {
	if err != nil {
		b.Fatal(err)
	}

	benchmarkSet(5000, engine, b)
}

func BenchmarkGet50(b *testing.B) {
	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(50, engine, b)
}

func BenchmarkGet500(b *testing.B) {	
	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(500, engine, b)
}

func BenchmarkGet5000(b *testing.B) {	
	if err != nil {
		b.Fatal(err)
	}

	benchmarkGet(5000, engine, b)
}
