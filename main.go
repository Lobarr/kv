package main

import (
	"flag"
	"log"
	"net"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"kv/core"
	"kv/protos"
)

var (
	httpServerType       = flag.String("http-server-type", "fiber", "HTTP server implementation to use (fiber or std)")
	segmentMaxSize       = flag.Int("segment_max_size", 1000000, "max size of entries stored in a data segment")
	cacheSize            = flag.Int("cache_size", 10000, "max number of data segments to hold in memory")
	shouldCompact        = flag.Bool("should_compact", false, "should the data be compacted")
	compactorWorkerCount = flag.Int("compactor_worker_count", 1, "number of workers compaction process uses")
)

func init() {
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)

	prometheus.Register(collectors.NewGoCollector())
}

func main() {
	flag.Parse()

	engine, err := core.NewEngine(&core.EngineConfig{
		SegmentMaxSize:             *segmentMaxSize,
		CacheSize:                  *cacheSize,
		DataPath:                   "/tmp/kv-data",
		ShouldCompact:              *shouldCompact,
		SnapshotInterval:           1 * time.Hour,
		CompactorInterval:          1 * time.Hour,
		CompactorWorkerCount:       *compactorWorkerCount,
		SnapshotTTLDuration:        1 * time.Hour,
		TolerableSnapshotFailCount: 5,
	})
	if err != nil {
		logrus.Fatal(err)
	}

	g := new(errgroup.Group)

	// Start gRPC server
	g.Go(func() error {
		lis, err := net.Listen("tcp", ":8081")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		protos.RegisterKVServer(s, core.NewGrpcServer(engine))
		log.Printf("grpc server listening at %v", lis.Addr())
		return s.Serve(lis)
	})

	// Start HTTP server
	g.Go(func() error {
		log.Printf("http server listening at :8080")
		if *httpServerType == "std" {
			httpServer := core.NewHttpStdServer(engine)
			return httpServer.Start(":8080")
		}
		httpServer := core.NewHttpServer(engine)
		return httpServer.Start(":8080")
	})

	if err := g.Wait(); err != nil {
		logrus.Fatal(err)
	}
}