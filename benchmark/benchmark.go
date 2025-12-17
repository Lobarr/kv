package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"kv/core"
	"kv/protos"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	mode       = flag.String("mode", "direct", "Benchmark mode: direct, http, grpc, experiment")
	grpcAddr   = flag.String("grpc-addr", ":50051", "gRPC server address")
	baseURL    = flag.String("base-url", "http://localhost:8080", "HTTP server base URL")
	dataPath   = flag.String("data-path", "./kv-data", "Path to store data")
	cpuProfile = flag.String("cpu-profile", "", "Write cpu profile to file")
)

// ServerHandle holds references to the running server components for graceful shutdown
type ServerHandle struct {
	Engine        *core.Engine
	GrpcServer    *grpc.Server
	HttpStdServer *core.HttpStdServer // Wrapper for std net/http
	HttpServer    *core.HttpServer    // Wrapper for gofiber
	DataPath      string
}

// parseURL parses and validates the base URL.
func parseURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	return u, nil
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// startKVServer starts the KV server (gRPC or HTTP) based on the mode and configuration.
// It returns a ServerHandle for managing the server's lifecycle.
func startKVServer(expConfig *ExperimentConfig, httpAddrOverride string) (*ServerHandle, error) {
	// Create a temporary data path for each server instance
	tempDataPath, err := os.MkdirTemp("", "kv-benchmark-data-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp data path: %w", err)
	}

	engineConfig := &core.EngineConfig{
		SegmentMaxSize:             expConfig.SegmentMaxSize,
		CacheSize:                  expConfig.CacheSize,
		DataPath:                   tempDataPath,
		ShouldCompact:              expConfig.ShouldCompact,
		CompactorWorkerCount:       expConfig.CompactorWorkerCount,
		SnapshotInterval:           1000 * time.Hour, // Disabled for bench
		CompactorInterval:          1 * time.Second,  // Enabled if ShouldCompact is true
		SnapshotTTLDuration:        1000 * time.Hour,
		TolerableSnapshotFailCount: 5,
	}

	engine, err := core.NewEngine(engineConfig)
	if err != nil {
		os.RemoveAll(tempDataPath) // Clean up if engine creation fails
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	serverHandle := &ServerHandle{
		Engine:   engine,
		DataPath: tempDataPath,
	}

	// Use errgroup to wait for server goroutines to start and report errors
	g, _ := errgroup.WithContext(context.Background())

	switch expConfig.Mode {
	case "grpc":
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			engine.Close()
			os.RemoveAll(tempDataPath)
			return nil, fmt.Errorf("failed to listen for gRPC: %v", err)
		}
		s := grpc.NewServer()
		protos.RegisterKVServer(s, core.NewGrpcServer(engine))
		serverHandle.GrpcServer = s

		g.Go(func() error {
			return s.Serve(lis)
		})
		time.Sleep(500 * time.Millisecond)

	case "http-std":
		var httpAddr string
		if httpAddrOverride != "" {
			httpAddr = httpAddrOverride
		} else {
			parsedURL, err := parseURL(*baseURL)
			if err != nil {
				engine.Close()
				os.RemoveAll(tempDataPath)
				return nil, fmt.Errorf("failed to parse base URL: %w", err)
			}
			httpAddr = parsedURL.Host
		}

		httpServer := core.NewHttpStdServer(engine)
		serverHandle.HttpStdServer = httpServer

		g.Go(func() error {
			return httpServer.Start(httpAddr)
		})
		time.Sleep(500 * time.Millisecond)

	case "http", "http-fiber": // "http" defaults to fiber for backward compatibility if needed, or we explicitly perform "http-fiber"
		var httpAddr string
		if httpAddrOverride != "" {
			httpAddr = httpAddrOverride
		} else {
			parsedURL, err := parseURL(*baseURL)
			if err != nil {
				engine.Close()
				os.RemoveAll(tempDataPath)
				return nil, fmt.Errorf("failed to parse base URL: %w", err)
			}
			httpAddr = parsedURL.Host
		}

		httpServer := core.NewHttpServer(engine)
		serverHandle.HttpServer = httpServer

		g.Go(func() error {
			return httpServer.Start(httpAddr)
		})
		time.Sleep(500 * time.Millisecond)

	default:
		// Direct mode does not start an external server.
	}

	return serverHandle, nil
}

// stopKVServer gracefully stops the KV server and cleans up resources.
func (sh *ServerHandle) stopKVServer() error {
	var errs []error

	if sh.GrpcServer != nil {
		sh.GrpcServer.GracefulStop()
	}

	if sh.HttpStdServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sh.HttpStdServer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown HTTP (std) server: %w", err))
		}
	}

	if sh.HttpServer != nil {
		if err := sh.HttpServer.Shutdown(); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown HTTP (Fiber) server: %w", err))
		}
	}

	if sh.Engine != nil {
		// Use a short delay/retry for closing engine if needed, but usually Close is enough.
		if err := sh.Engine.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close engine: %w", err))
		}
	}

	if sh.DataPath != "" {
		if err := os.RemoveAll(sh.DataPath); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove temporary data path %s: %w", sh.DataPath, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during server shutdown: %v", errs)
	}
	return nil
}

// ... (benchmarkStats and helpers remain same) ...

// (Duplicate code removed)

type benchmarkStats struct {
	setCount          atomic.Int64
	getCount          atomic.Int64
	setErrors         atomic.Int64
	getErrors         atomic.Int64
	totalSetBytes     atomic.Int64
	totalGetBytes     atomic.Int64
	totalWriteTime    atomic.Int64 // nanoseconds spent on SET operations
	totalReadTime     atomic.Int64 // nanoseconds spent on GET operations
	writePhaseElapsed atomic.Int64 // wall-clock nanoseconds for write phase
	readPhaseElapsed  atomic.Int64 // wall-clock nanoseconds for read phase
	readLatencies     []time.Duration
	writeLatencies    []time.Duration
	mu                sync.Mutex
}

func (s *benchmarkStats) Reset() {
	s.setCount.Store(0)
	s.getCount.Store(0)
	s.setErrors.Store(0)
	s.getErrors.Store(0)
	s.totalSetBytes.Store(0)
	s.totalGetBytes.Store(0)
	s.totalWriteTime.Store(0)
	s.totalReadTime.Store(0)
	s.writePhaseElapsed.Store(0)
	s.readPhaseElapsed.Store(0)
	s.readLatencies = nil
	s.writeLatencies = nil
}

func (s *benchmarkStats) RecordReadLatency(d time.Duration) {
	s.mu.Lock()
	s.readLatencies = append(s.readLatencies, d)
	s.mu.Unlock()
}

func (s *benchmarkStats) RecordWriteLatency(d time.Duration) {
	s.mu.Lock()
	s.writeLatencies = append(s.writeLatencies, d)
	s.mu.Unlock()
}

// ExperimentConfig defines the engine-specific configuration and workload for an experiment.
type ExperimentConfig struct {
	Name                 string
	Mode                 string // "direct", "http", or "grpc"
	SegmentMaxSize       int
	CacheSize            int
	ShouldCompact        bool
	CompactorWorkerCount int
	NumRequests          int
	Concurrency          int
	BatchSize            int // Used for HTTP/gRPC batch operations
}

type ExperimentResult struct {
	Config          ExperimentConfig
	Throughput      float64
	ReadThroughput  float64
	WriteThroughput float64
	Latency         float64 // Avg latency
	LatP50          float64
	LatP99          float64
	LatP999         float64
	Status          string
}

// Result sorting helpers
type byThroughput []ExperimentResult

func (a byThroughput) Len() int           { return len(a) }
func (a byThroughput) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byThroughput) Less(i, j int) bool { return a[i].Throughput < a[j].Throughput }

func (a byThroughput) Reverse() []ExperimentResult {
	res := make([]ExperimentResult, len(a))
	for i, j := 0, len(a)-1; i < len(a); i, j = i+1, j-1 {
		res[i] = a[j]
	}
	return res
}

type byLatencyP99 []ExperimentResult

func (a byLatencyP99) Len() int           { return len(a) }
func (a byLatencyP99) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLatencyP99) Less(i, j int) bool { return a[i].LatP99 < a[j].LatP99 }

var baseExperiments = []ExperimentConfig{
	// --- DIRECT MODE ---
	// 1. Batch Size Variations (Fix Conc=500)
	{Name: "Direct Batch 1", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 1},
	{Name: "Direct Batch 10", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 10},
	{Name: "Direct Batch 50", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 50},
	{Name: "Direct Batch 100 (Base)", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Direct Batch 200", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 200},
	{Name: "Direct Batch 500", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 500},
	{Name: "Direct Batch 1000", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 1000},

	// 2. Concurrency Variations (Fix Batch=100)
	{Name: "Direct Conc 10", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 50000, Concurrency: 10, BatchSize: 100},
	{Name: "Direct Conc 50", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 250000, Concurrency: 50, BatchSize: 100},
	{Name: "Direct Conc 100", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 500000, Concurrency: 100, BatchSize: 100},
	{Name: "Direct Conc 1000", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 1000, BatchSize: 100},
	{Name: "Direct Conc 2000", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 2000, BatchSize: 100},
	{Name: "Direct Conc 5000", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 5000, BatchSize: 100},

	// 3. Segment Size (Fix Batch=100, Conc=500)
	{Name: "Direct Seg 1MB", Mode: "direct", SegmentMaxSize: 1024 * 1024, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Direct Seg 10MB", Mode: "direct", SegmentMaxSize: 10 * 1024 * 1024, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Direct Seg 100MB", Mode: "direct", SegmentMaxSize: 100 * 1024 * 1024, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},

	// 4. Cache Size (Fix Batch=100, Conc=500, Seg=1MB)
	{Name: "Direct Cache 1K", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 1000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Direct Cache 100K", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 100000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Direct Cache 1M", Mode: "direct", SegmentMaxSize: 1000000, CacheSize: 1000000, ShouldCompact: false, CompactorWorkerCount: 1, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},

	// --- gRPC MODE ---
	// 1. Batch Size Variations
	{Name: "gRPC Batch 1", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 100000, Concurrency: 500, BatchSize: 1},
	{Name: "gRPC Batch 10", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 500000, Concurrency: 500, BatchSize: 10},
	{Name: "gRPC Batch 50", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 50},
	{Name: "gRPC Batch 100", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "gRPC Batch 500", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 500},
	{Name: "gRPC Batch 1000", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 1000},

	// 2. Concurrency
	{Name: "gRPC Conc 100", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 500000, Concurrency: 100, BatchSize: 100},
	{Name: "gRPC Conc 1000", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 1000, BatchSize: 100},
	{Name: "gRPC Conc 2000", Mode: "grpc", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 2000, BatchSize: 100},

	// --- HTTP MODE ---
	// 1. Fiber Batching
	{Name: "Fiber Batch 1", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 100000, Concurrency: 500, BatchSize: 1},
	{Name: "Fiber Batch 50", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 50},
	{Name: "Fiber Batch 100", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
	{Name: "Fiber Batch 500", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 500},
	{Name: "Fiber Batch 1000", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 1000},

	// 2. Fiber Concurrency
	{Name: "Fiber Conc 1000", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 1000, BatchSize: 100},
	{Name: "Fiber Conc 2000", Mode: "http-fiber", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 2000, BatchSize: 100},

	// 3. Std Comparison
	{Name: "Std Batch 100", Mode: "http-std", SegmentMaxSize: 1000000, CacheSize: 10000, ShouldCompact: false, CompactorWorkerCount: 0, NumRequests: 1000000, Concurrency: 500, BatchSize: 100},
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

// Direct mode functions
func setKeyDirect(engine core.Store, key, value string, stats *benchmarkStats) error {
	if err := engine.Set(key, value); err != nil {
		stats.setErrors.Add(1)
		return err
	}
	stats.setCount.Add(1)
	stats.totalSetBytes.Add(int64(len(key) + len(value)))
	return nil
}

func getKeyDirect(engine core.Store, key string, expectedValue string, stats *benchmarkStats) error {
	gotValue, err := engine.Get(key)
	if err != nil {
		stats.getErrors.Add(1)
		return err
	}
	if gotValue != expectedValue {
		stats.getErrors.Add(1)
		return fmt.Errorf("value mismatch: expected %s, got %s", expectedValue, gotValue)
	}
	stats.getCount.Add(1)
	stats.totalGetBytes.Add(int64(len(gotValue)))
	return nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatNumber formats large numbers with K/M/B suffixes for readability
func formatNumber(n float64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.2fB", n/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.2fM", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fK", n/1e3)
	default:
		return fmt.Sprintf("%.0f", n)
	}
}

func runHTTPBatchBenchmark(stats *benchmarkStats, numRequests int, concurrency int, batchSizeParam int, targetBaseURL string) error {
	// Create a custom client to avoid connection reuse issues between server restarts
	// IMPORTANT: Enable KeepAlives but set generous pool limits to match concurrency.
	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   false,
			MaxIdleConns:        concurrency + 10,
			MaxIdleConnsPerHost: concurrency + 10,
		},
		Timeout: 30 * time.Second,
	}
	defer client.CloseIdleConnections()

	// Phase 1: Write all keys
	writeStart := time.Now()
	var wgWrite errgroup.Group
	wgWrite.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgWrite.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			// Batch SET
			setItems := make([]map[string]string, currentBatchSize)
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				setItems[j] = map[string]string{"key": key, "value": value}
			}

			startSet := time.Now()
			setBody, _ := json.Marshal(map[string]interface{}{"items": setItems})
			resp, err := client.Post(fmt.Sprintf("%s/batch/set", targetBaseURL), "application/json", bytes.NewReader(setBody))
			if err != nil {
				stats.setErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch set: %v", err)
			}
			resp.Body.Close()
			setElapsed := time.Since(startSet)
			stats.totalWriteTime.Add(setElapsed.Nanoseconds())
			stats.RecordWriteLatency(setElapsed)
			if resp.StatusCode != http.StatusOK {
				stats.setErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch set: status code %d", resp.StatusCode)
			}
			stats.setCount.Add(int64(currentBatchSize))
			for _, item := range setItems {
				stats.totalSetBytes.Add(int64(len(item["key"]) + len(item["value"])))
			}

			return nil
		})
	}
	if err := wgWrite.Wait(); err != nil {
		return err
	}
	stats.writePhaseElapsed.Store(time.Since(writeStart).Nanoseconds())

	// Phase 2: Read all keys
	readStart := time.Now()
	var wgRead errgroup.Group
	wgRead.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgRead.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			// Batch GET
			getKeys := make([]string, currentBatchSize)
			expectedValues := make(map[string]string, currentBatchSize)
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				getKeys[j] = key
				expectedValues[key] = value
			}

			startGet := time.Now()
			getBody, _ := json.Marshal(map[string]interface{}{"keys": getKeys})
			resp, err := client.Post(fmt.Sprintf("%s/batch/get", targetBaseURL), "application/json", bytes.NewReader(getBody))
			if err != nil {
				stats.getErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch get: %v", err)
			}
			defer resp.Body.Close()
			getElapsed := time.Since(startGet)
			stats.totalReadTime.Add(getElapsed.Nanoseconds())
			stats.RecordReadLatency(getElapsed)

			if resp.StatusCode != http.StatusOK {
				stats.getErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch get: status code %d", resp.StatusCode)
			}

			var getResp struct {
				Items []struct {
					Key   string `json:"key"`
					Value string `json:"value"`
				} `json:"items"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
				stats.getErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to decode get response: %v", err)
			}

			stats.getCount.Add(int64(len(getResp.Items)))
			for _, item := range getResp.Items {
				if expectedValues[item.Key] != item.Value {
					stats.getErrors.Add(1)
				}
				stats.totalGetBytes.Add(int64(len(item.Value)))
			}

			return nil
		})
	}
	if err := wgRead.Wait(); err != nil {
		return err
	}
	stats.readPhaseElapsed.Store(time.Since(readStart).Nanoseconds())
	return nil
}

func runDirectBenchmark(stats *benchmarkStats, config *core.EngineConfig, numRequests int, concurrency int, batchSizeParam int) error {
	// Create engine directly
	engine, err := core.NewEngine(config)
	if err != nil {
		return err
	}
	defer engine.Close()

	// Phase 1: Write all keys
	writeStart := time.Now()
	var wgWrite errgroup.Group
	wgWrite.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgWrite.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			startSet := time.Now()
			// Batch Loop for Direct
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				if err := setKeyDirect(engine, key, value, stats); err != nil {
					return err
				}
			}
			setElapsed := time.Since(startSet)
			stats.totalWriteTime.Add(setElapsed.Nanoseconds())
			stats.RecordWriteLatency(setElapsed) // This records latency for the BATCH loop (similar to HTTP/gRPC)
			return nil
		})
	}
	if err := wgWrite.Wait(); err != nil {
		return err
	}
	stats.writePhaseElapsed.Store(time.Since(writeStart).Nanoseconds())

	// Sync buffered writes to disk before reading
	if err := engine.Sync(); err != nil {
		return err
	}

	// Phase 2: Read all keys
	readStart := time.Now()
	var wgRead errgroup.Group
	wgRead.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgRead.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			startGet := time.Now()
			// Batch Loop for Direct
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				if err := getKeyDirect(engine, key, value, stats); err != nil {
					return err
				}
			}
			getElapsed := time.Since(startGet)
			stats.totalReadTime.Add(getElapsed.Nanoseconds())
			stats.RecordReadLatency(getElapsed) // Records BATCH latency
			return nil
		})
	}
	err = wgRead.Wait()
	stats.readPhaseElapsed.Store(time.Since(readStart).Nanoseconds())

	return err
}

func runGRPCBenchmark(stats *benchmarkStats, numRequests int, concurrency int, batchSizeParam int) error {
	conn, err := grpc.Dial(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()
	client := protos.NewKVClient(conn)

	// Phase 1: Write all keys
	writeStart := time.Now()
	var wgWrite errgroup.Group
	wgWrite.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgWrite.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			// Batch SET
			setRequests := make([]*protos.SetRequest, currentBatchSize)
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				setRequests[j] = &protos.SetRequest{Key: key, Value: value}
			}

			startSet := time.Now()
			_, err := client.BatchSet(context.Background(), &protos.BatchSetRequest{Requests: setRequests})
			if err != nil {
				stats.setErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch set: %v", err)
			}
			setElapsed := time.Since(startSet)
			stats.totalWriteTime.Add(setElapsed.Nanoseconds())
			stats.RecordWriteLatency(setElapsed)
			stats.setCount.Add(int64(currentBatchSize))
			for _, req := range setRequests {
				stats.totalSetBytes.Add(int64(len(req.Key) + len(req.Value)))
			}
			return nil
		})
	}
	if err := wgWrite.Wait(); err != nil {
		return err
	}
	stats.writePhaseElapsed.Store(time.Since(writeStart).Nanoseconds())

	// Phase 2: Read all keys
	readStart := time.Now()
	var wgRead errgroup.Group
	wgRead.SetLimit(concurrency)

	for i := 0; i < numRequests; i += batchSizeParam {
		i := i
		wgRead.Go(func() error {
			currentBatchSize := batchSizeParam
			if i+currentBatchSize > numRequests {
				currentBatchSize = numRequests - i
			}

			// Batch GET
			getRequests := make([]*protos.GetRequest, currentBatchSize)
			expectedValues := make([]string, currentBatchSize)
			for j := 0; j < currentBatchSize; j++ {
				idx := i + j
				key := fmt.Sprintf("key-%09d", idx)
				value := fmt.Sprintf("val-%09d", idx)
				getRequests[j] = &protos.GetRequest{Key: key}
				expectedValues[j] = value
			}

			startGet := time.Now()
			getResponse, err := client.BatchGet(context.Background(), &protos.BatchGetRequest{Requests: getRequests})
			if err != nil {
				stats.getErrors.Add(int64(currentBatchSize))
				return fmt.Errorf("failed to batch get: %v", err)
			}
			getElapsed := time.Since(startGet)
			stats.totalReadTime.Add(getElapsed.Nanoseconds())
			stats.RecordReadLatency(getElapsed)
			stats.getCount.Add(int64(currentBatchSize))
			for i, resp := range getResponse.Responses {
				if resp.Value != expectedValues[i] {
					stats.getErrors.Add(1)
				}
				stats.totalGetBytes.Add(int64(len(resp.Value)))
			}

			return nil
		})
	}
	if err := wgRead.Wait(); err != nil {
		return err
	}
	stats.readPhaseElapsed.Store(time.Since(readStart).Nanoseconds())
	return nil
}

func printResults(elapsed time.Duration, stats *benchmarkStats, benchmarkMode string, concurrency int, batchSizeVal int) {
	fmt.Println("\n========================================")
	switch benchmarkMode {
	case "http":
		fmt.Println("      HTTP BENCHMARK RESULTS")
	case "direct":
		fmt.Println("    DIRECT ENGINE BENCHMARK RESULTS")
	case "experiment":
		fmt.Println("    EXPERIMENT BENCHMARK RESULTS")
	default: // grpc
		fmt.Println("       gRPC BENCHMARK RESULTS")
	}
	fmt.Println("========================================")
	fmt.Printf("Mode:                    %s\n", benchmarkMode)
	fmt.Printf("Duration:                %s\n", elapsed)
	fmt.Printf("Concurrency:             %d\n", concurrency)
	// Batch size is only relevant for HTTP and gRPC modes
	if benchmarkMode == "grpc" || benchmarkMode == "http" {
		fmt.Printf("Batch Size:              %d\n", batchSizeVal)
	}
	fmt.Println()

	fmt.Println("SET Operations:")
	fmt.Printf("  Total:                 %d\n", stats.setCount.Load())
	fmt.Printf("  Errors:                %d\n", stats.setErrors.Load())
	fmt.Printf("  Throughput:            %.2f ops/sec\n", float64(stats.setCount.Load())/elapsed.Seconds())
	fmt.Printf("  Data Written:          %s\n", formatBytes(stats.totalSetBytes.Load()))
	fmt.Printf("  Write Bandwidth:       %s/sec\n", formatBytes(int64(float64(stats.totalSetBytes.Load())/elapsed.Seconds())))
	fmt.Println()

	fmt.Println("GET Operations:")
	fmt.Printf("  Total:                 %d\n", stats.getCount.Load())
	fmt.Printf("  Errors:                %d\n", stats.getErrors.Load())
	fmt.Printf("  Throughput:            %.2f ops/sec\n", float64(stats.getCount.Load())/elapsed.Seconds())
	fmt.Printf("  Data Read:             %s\n", formatBytes(stats.totalGetBytes.Load()))
	fmt.Printf("  Read Bandwidth:        %s/sec\n", formatBytes(int64(float64(stats.totalGetBytes.Load())/elapsed.Seconds())))
	fmt.Println()

	totalOps := stats.setCount.Load() + stats.getCount.Load()
	totalErrors := stats.setErrors.Load() + stats.getErrors.Load()
	totalBytes := stats.totalSetBytes.Load() + stats.totalGetBytes.Load()

	fmt.Println("OVERALL:")
	fmt.Printf("  Total Operations:      %d\n", totalOps)
	fmt.Printf("  Total Errors:          %d\n", totalErrors)
	if totalOps+totalErrors > 0 {
		fmt.Printf("  Success Rate:          %.2f%%\n", 100.0*float64(totalOps)/float64(totalOps+totalErrors))
	}
	fmt.Printf("  Overall Throughput:    %.2f ops/sec\n", float64(totalOps)/elapsed.Seconds())
	fmt.Printf("  Total Data Transfer:   %s\n", formatBytes(totalBytes))
	fmt.Printf("  Overall Bandwidth:     %s/sec\n", formatBytes(int64(float64(totalBytes)/elapsed.Seconds())))
	if totalOps > 0 {
		fmt.Printf("  Avg Latency:           %.2f ms\n", elapsed.Seconds()*1000.0/float64(totalOps))
	}
	fmt.Println("========================================")
}

func runExperiments() {
	logrus.SetLevel(logrus.ErrorLevel)

	fmt.Println("Starting Experiments...")
	fmt.Println("+-----+---------------------------+--------+-------------+-----------+---------+---------+-------+------+--------------------+--------------------+------------------+------------------+----------+")
	fmt.Printf("| %-3s | %-25s | %-6s | %-11s | %-9s | %-7s | %-7s | %-5s | %-4s | %-18s | %-18s | %-16s | %-16s | %-8s |\n",
		"ID", "Config Name", "Mode", "SegmentSize", "CacheSize", "Compact", "Workers", "Batch", "Conc", "Read Tput (op/s)", "Write Tput (op/s)", "Read Lat (A/P99)", "Write Lat (A/P99)", "Status")
	fmt.Println("+-----+---------------------------+--------+-------------+-----------+---------+---------+-------+------+--------------------+--------------------+------------------+------------------+----------+")

	var results []ExperimentResult
	experimentID := 1
	for _, expConfig := range baseExperiments {
		reqs := expConfig.NumRequests
		conc := expConfig.Concurrency
		expMode := expConfig.Mode
		if expMode == "" {
			expMode = "direct" // default to direct mode
		}

		// Clean up previous data (for direct mode)
		if expMode == "direct" {
			os.RemoveAll(*dataPath)
			if err := os.MkdirAll(*dataPath, 0777); err != nil {
				fmt.Printf("| %-3d | %-25s | %-6s | %-11d | %-9d | %-7v | %-7d | %-5d | %-4d | %-18s | %-18s | %-16s | %-16s | %-8s |\n",
					experimentID, expConfig.Name, expMode, expConfig.SegmentMaxSize, expConfig.CacheSize,
					expConfig.ShouldCompact, expConfig.CompactorWorkerCount, expConfig.BatchSize, conc, "N/A", "N/A", "N/A", "N/A", "Error")
				experimentID++
				continue
			}
		}

		stats := &benchmarkStats{}
		var err error
		var serverHandle *ServerHandle

		start := time.Now()

		switch expMode {
		case "http", "http-fiber", "http-std", "grpc":
			var httpAddrOverride string
			var targetBaseURL string = *baseURL

			if expMode == "http" || expMode == "http-fiber" || expMode == "http-std" {
				// Allocate a dynamic port for HTTP experiments to avoid conflicts
				port, err := getFreePort()
				if err != nil {
					fmt.Printf("Experiment %s failed: failed to get free port: %v\n", expConfig.Name, err)
					continue
				}
				httpAddrOverride = fmt.Sprintf("localhost:%d", port)
				targetBaseURL = fmt.Sprintf("http://%s", httpAddrOverride)
			}

			// Start server for this experiment
			serverHandle, err = startKVServer(&expConfig, httpAddrOverride)
			if err != nil {
				err = fmt.Errorf("server start failed: %w", err)
				fmt.Printf("Error starting server for %s: %v\n", expConfig.Name, err)
			} else {
				// Run benchmark against the started server
				if expMode == "http" || expMode == "http-fiber" || expMode == "http-std" {
					err = runHTTPBatchBenchmark(stats, reqs, conc, expConfig.BatchSize, targetBaseURL)
				} else {
					err = runGRPCBenchmark(stats, reqs, conc, expConfig.BatchSize)
				}

				if err != nil {
					fmt.Printf("Benchmark %s failed: %v\n", expConfig.Name, err)
				}

				// Stop server after benchmark
				if stopErr := serverHandle.stopKVServer(); stopErr != nil {
					// Ignore errors during cleanup
				}
			}
			// Sleep to ensure ports are released
			time.Sleep(2 * time.Second)

		default: // direct
			config := &core.EngineConfig{
				SegmentMaxSize:             expConfig.SegmentMaxSize,
				CacheSize:                  expConfig.CacheSize,
				DataPath:                   *dataPath,
				ShouldCompact:              expConfig.ShouldCompact,
				CompactorWorkerCount:       expConfig.CompactorWorkerCount,
				SnapshotInterval:           1000 * time.Hour, // Disabled for bench
				CompactorInterval:          1 * time.Second,  // Enabled if ShouldCompact is true
				SnapshotTTLDuration:        1000 * time.Hour,
				TolerableSnapshotFailCount: 5,
			}
			err = runDirectBenchmark(stats, config, reqs, conc, expConfig.BatchSize)
		}
		elapsed := time.Since(start)

		totalOps := stats.setCount.Load() + stats.getCount.Load()
		readLatStats := calculateLatencyStatsFromSlice(stats.readLatencies)
		writeLatStats := calculateLatencyStatsFromSlice(stats.writeLatencies)

		// Calculate throughput based on phase-specific wall-clock elapsed time
		elapsedSec := elapsed.Seconds()
		overallTput := float64(totalOps) / elapsedSec

		// Use phase-specific elapsed times for independent throughput
		var readTput, writeTput float64
		if stats.readPhaseElapsed.Load() > 0 {
			readTput = float64(stats.getCount.Load()) / (float64(stats.readPhaseElapsed.Load()) / 1e9)
		} else {
			readTput = float64(stats.getCount.Load()) / elapsedSec
		}
		if stats.writePhaseElapsed.Load() > 0 {
			writeTput = float64(stats.setCount.Load()) / (float64(stats.writePhaseElapsed.Load()) / 1e9)
		} else {
			writeTput = float64(stats.setCount.Load()) / elapsedSec
		}

		result := ExperimentResult{
			Config:          expConfig,
			Throughput:      overallTput,
			ReadThroughput:  readTput,
			WriteThroughput: writeTput,
			Latency:         (readLatStats.Avg + writeLatStats.Avg) / 2, // Combined avg
			LatP50:          (readLatStats.P50 + writeLatStats.P50) / 2,
			LatP99:          (readLatStats.P99 + writeLatStats.P99) / 2,
			LatP999:         (readLatStats.P999 + writeLatStats.P999) / 2,
			Status:          "Success",
		}
		if err != nil {
			result.Status = "Failed"
			result.Throughput = 0
			result.ReadThroughput = 0
			result.WriteThroughput = 0
		}
		results = append(results, result)

		// Format: Read Lat / Write Lat (Avg/P99)
		readLatStr := fmt.Sprintf("%.2f / %.2f", readLatStats.Avg, readLatStats.P99)
		writeLatStr := fmt.Sprintf("%.2f / %.2f", writeLatStats.Avg, writeLatStats.P99)
		statusStr := result.Status
		if err != nil {
			statusStr = "Fail" // Shorten for table
		}

		fmt.Printf("| %-3d | %-25s | %-6s | %-11d | %-9d | %-7v | %-7d | %-5d | %-4d | %-18s | %-18s | %-16s | %-16s | %-8s |\n",
			experimentID, expConfig.Name, expMode, expConfig.SegmentMaxSize, expConfig.CacheSize,
			expConfig.ShouldCompact, expConfig.CompactorWorkerCount, expConfig.BatchSize, conc, formatNumber(result.ReadThroughput), formatNumber(result.WriteThroughput), readLatStr, writeLatStr, statusStr)
		experimentID++
	}

	// Cleanup after last run (direct mode path)
	if _, err := os.Stat(*dataPath); err == nil {
		os.RemoveAll(*dataPath)
	}
	fmt.Println("+-----+---------------------------+--------+-------------+-----------+---------+---------+-------+------+--------------------+-----------------------------+----------+")
	fmt.Println("Experiments completed.")
	printExperimentAnalysis(results)
}

func printExperimentAnalysis(results []ExperimentResult) {
	fmt.Println("\nPERFORMANCE ANALYSIS")
	fmt.Println("====================")

	// Filter successful results
	var successResults []ExperimentResult
	for _, r := range results {
		if r.Status == "Success" {
			successResults = append(successResults, r)
		}
	}

	if len(successResults) == 0 {
		fmt.Println("No successful experiments to analyze.")
		return
	}

	// 1. Top 3 Performers
	sort.Sort(byThroughput(successResults))
	topResults := byThroughput(successResults).Reverse()

	fmt.Println("\nTop 3 Configurations (Throughput):")
	for i := 0; i < 3 && i < len(topResults); i++ {
		r := topResults[i]
		fmt.Printf("  %d. %-25s (%s): Read: %s/s, Write: %s/s (Lat Avg/P99: %.2f / %.2f ms)\n",
			i+1, r.Config.Name, r.Config.Mode, formatNumber(r.ReadThroughput), formatNumber(r.WriteThroughput), r.Latency, r.LatP99)
	}

	// 2. Top 3 Lowest Latency (P99)
	sort.Sort(byLatencyP99(successResults))

	fmt.Println("\nTop 3 Configurations (Lowest P99 Latency):")
	for i := 0; i < 3 && i < len(successResults); i++ {
		r := successResults[i]
		fmt.Printf("  %d. %-25s (%s): %.2f ms (P99) - Read: %s/s, Write: %s/s\n",
			i+1, r.Config.Name, r.Config.Mode, r.LatP99, formatNumber(r.ReadThroughput), formatNumber(r.WriteThroughput))
	}

	// 3. Mode Comparison
	fmt.Println("\nMode Comparison (Avg Throughput):")
	modeStats := make(map[string]struct {
		readSum  float64
		writeSum float64
		count    int
	})
	for _, r := range successResults {
		mode := r.Config.Mode
		if mode == "" {
			mode = "direct"
		}

		s := modeStats[mode]
		s.readSum += r.ReadThroughput
		s.writeSum += r.WriteThroughput
		s.count++
		modeStats[mode] = s
	}

	var modes []string
	for m := range modeStats {
		modes = append(modes, m)
	}
	sort.Strings(modes)

	for _, m := range modes {
		stats := modeStats[m]
		if stats.count > 0 {
			avgRead := stats.readSum / float64(stats.count)
			avgWrite := stats.writeSum / float64(stats.count)
			fmt.Printf("  %-8s: Read: %s/s, Write: %s/s (across %d configs)\n", m, formatNumber(avgRead), formatNumber(avgWrite), stats.count)
		}
	}
}

type LatencyStats struct {
	Avg  float64
	P50  float64
	P99  float64
	P999 float64
}

func calculateLatencyStatsFromSlice(samples []time.Duration) LatencyStats {
	count := len(samples)
	if count == 0 {
		return LatencyStats{}
	}

	// Calculate Avg
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}
	avg := float64(sum.Microseconds()) / 1000.0 / float64(count)

	// Sort for percentiles
	// Copy slice to avoid race if concurrent writes still happen (though benchmark is done here)
	sorted := make([]time.Duration, count)
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 := float64(sorted[count*50/100].Microseconds()) / 1000.0
	p99 := float64(sorted[count*99/100].Microseconds()) / 1000.0
	p999 := float64(sorted[count*999/1000].Microseconds()) / 1000.0

	return LatencyStats{Avg: avg, P50: p50, P99: p99, P999: p999}
}

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *mode != "http" && *mode != "direct" && *mode != "grpc" && *mode != "experiment" {
		log.Fatalf("Invalid mode: %s. Must be 'http', 'direct', 'grpc' or 'experiment'", *mode)
	}

	if *mode == "experiment" {
		runExperiments()
		return
	}

	// Default values for non-experiment modes
	const defaultNumRequests = 1000000
	const defaultConcurrency = 1000
	const defaultBatchSize = 100

	stats := &benchmarkStats{}
	start := time.Now()

	var err error
	switch *mode {
	case "http":
		err = runHTTPBatchBenchmark(stats, defaultNumRequests, defaultConcurrency, defaultBatchSize, *baseURL)
	case "direct":
		config := &core.EngineConfig{
			SegmentMaxSize:             1000000,
			CacheSize:                  10000,
			DataPath:                   *dataPath,
			ShouldCompact:              false,
			SnapshotInterval:           1000 * time.Hour,
			CompactorInterval:          1000 * time.Hour,
			CompactorWorkerCount:       1,
			SnapshotTTLDuration:        1000 * time.Hour,
			TolerableSnapshotFailCount: 5,
		}
		err = runDirectBenchmark(stats, config, defaultNumRequests, defaultConcurrency, defaultBatchSize)
	case "grpc":
		err = runGRPCBenchmark(stats, defaultNumRequests, defaultConcurrency, defaultBatchSize)
	}

	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	elapsed := time.Since(start)
	printResults(elapsed, stats, *mode, defaultConcurrency, defaultBatchSize)
}
