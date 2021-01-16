package main

import (
	"fmt"
	"storage-engines/engines/hashindex"
	"sync"
	"time"
)

func main() {
	//TODO: add actual tests
	engine, err := hashindex.NewEngine(&hashindex.EngineConfig{
		SegmentMaxSize:             2000,
		SnapshotInterval:           2 * time.Second,
		TolerableSnapshotFailCount: 5,
		CacheSize:                  10,
		CompactorInterval:          1 * time.Second,
		CompactorWorkerCount:       3,
		SnapshotTTLDuration:        4 * time.Second,
	})

	if err != nil {
		panic(err)
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 500; i++ {
		go func(wg *sync.WaitGroup, id int) {
			for i := 0; i < 100; i++ {
				if err := engine.Set("some-key", "some-value"); err != nil {
					fmt.Println(err)
				}
				if err := engine.Set("some-key", "new-value"); err != nil {
					fmt.Println(err)
				}
				if err := engine.Set("json", "{'ping': 'pong'}"); err != nil {
					fmt.Println()
				}
			}
			wg.Done()
		}(wg, i)
		wg.Add(1)
	}

	wg.Wait()
	value, _ := engine.Get("some-key")
	jsonValue, _ := engine.Get("json")

	engine.Close()

	fmt.Println(value)
	fmt.Println(jsonValue)
}
