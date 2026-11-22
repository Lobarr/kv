package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"kv/core"
	"kv/protos"
)

var (
	grpcAddr             = flag.String("grpc-addr", ":8081", "address for gRPC server")
	metricsAddr          = flag.String("metrics-addr", ":9999", "address for /health and Prometheus /metrics")
	segmentMaxSize       = flag.Int("segment_max_size", 1000000, "max size of entries stored in a data segment")
	cacheSize            = flag.Int("cache_size", 10000, "max number of data segments to hold in memory")
	shouldCompact        = flag.Bool("should_compact", false, "should the data be compacted")
	compactorWorkerCount = flag.Int("compactor_worker_count", 1, "number of workers compaction process uses")
	dataPath             = flag.String("data-path", "/tmp/kv-data", "path where store data is persisted on disk")
	enableCompression    = flag.Bool("enable_compression", false, "whether to enable compression or not")
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
		DataPath:                   *dataPath,
		ShouldCompact:              *shouldCompact,
		EnableCompression:          *enableCompression,
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

	grpcServer := grpc.NewServer()

	// Start gRPC server
	g.Go(func() error {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			return err
		}
		protos.RegisterKVServer(grpcServer, core.NewGrpcServer(engine))
		log.Printf("grpc server listening at %v", lis.Addr())
		return grpcServer.Serve(lis)
	})

	// Start admin server (/health + Prometheus /metrics)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	adminMux.Handle("/metrics", promhttp.Handler())
	adminServer := &http.Server{
		Addr:    *metricsAddr,
		Handler: adminMux,
	}

	g.Go(func() error {
		log.Printf("admin server listening at %s (/health, /metrics)", *metricsAddr)
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	serverErr := make(chan error, 1)
	go func() { serverErr <- g.Wait() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if closeErr := engine.Close(); closeErr != nil {
			logrus.Errorf("engine close error: %v", closeErr)
		}
		if err != nil {
			logrus.Fatal(err)
		}

	case received := <-sig:
		logrus.Infof("received signal %v, shutting down", received)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = adminServer.Shutdown(shutdownCtx)

		grpcDone := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(grpcDone)
		}()
		select {
		case <-grpcDone:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}

		if err := engine.Close(); err != nil {
			logrus.Errorf("engine close error: %v", err)
		}
	}
}
