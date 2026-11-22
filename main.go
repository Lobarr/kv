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
	httpServerType       = flag.String("http-server-type", "fiber", "HTTP server implementation to use (fiber or std)")
	segmentMaxSize       = flag.Int("segment_max_size", 1000000, "max size of entries stored in a data segment")
	cacheSize            = flag.Int("cache_size", 10000, "max number of data segments to hold in memory")
	shouldCompact        = flag.Bool("should_compact", false, "should the data be compacted")
	compactorWorkerCount = flag.Int("compactor_worker_count", 1, "number of workers compaction process uses")
	dataPath             = flag.String("data-path", "/tmp/kv-data", "path where store data is persisted on disk")
	enableCompression    = flag.Bool("enable_compression", false, "whether to enable compression or not")
	metricsAddr          = flag.String("metrics-addr", ":9999", "address to expose Prometheus /metrics endpoint")
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
		lis, err := net.Listen("tcp", ":8081")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		protos.RegisterKVServer(grpcServer, core.NewGrpcServer(engine))
		log.Printf("grpc server listening at %v", lis.Addr())
		return grpcServer.Serve(lis)
	})

	httpServer := core.NewHttpServer(engine)
	httpStdServer := core.NewHttpStdServer(engine)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    *metricsAddr,
		Handler: metricsMux,
	}

	// Start Prometheus metrics server
	g.Go(func() error {
		log.Printf("metrics server listening at %s/metrics", *metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Start HTTP server
	g.Go(func() error {
		log.Printf("http server listening at :8080")
		if *httpServerType == "std" {
			return httpStdServer.Start(":8080")
		}
		return httpServer.Start(":8080")
	})

	// Wait for the servers in the background; we coordinate shutdown below so
	// the engine is always closed before the process exits.
	serverErr := make(chan error, 1)
	go func() { serverErr <- g.Wait() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		// Servers stopped on their own (or failed): still flush the engine.
		if closeErr := engine.Close(); closeErr != nil {
			logrus.Errorf("engine close error: %v", closeErr)
		}
		if err != nil {
			logrus.Fatal(err)
		}

	case received := <-sig:
		logrus.Infof("received signal %v, shutting down", received)

		// Stop accepting new connections (bounded), then flush and close the
		// engine so a final snapshot is always persisted before exit.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if *httpServerType == "std" {
			if err := httpStdServer.Shutdown(shutdownCtx); err != nil {
				logrus.Errorf("http server shutdown error: %v", err)
			}
		} else {
			if err := httpServer.Shutdown(); err != nil {
				logrus.Errorf("http server shutdown error: %v", err)
			}
		}
		_ = metricsServer.Shutdown(shutdownCtx)

		// GracefulStop waits for in-flight RPCs; fall back to a hard stop.
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
			<-stopped
		}

		if closeErr := engine.Close(); closeErr != nil {
			logrus.Errorf("engine close error: %v", closeErr)
		}

		// Engine state is flushed; the servers are already stopped so the
		// errgroup returns promptly. Exit without waiting on it.
		os.Exit(0)
	}
}
