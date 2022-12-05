package core

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/helmet/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sherifabdlnaby/configuro"
	"github.com/sirupsen/logrus"
)

const ConfigPath = "/kv/config.yaml"

type KvHttpServer struct {
	server *fiber.App          // http server instance
	store  Store               // store instance
	config *KvHttpServerConfig // server configuration
}

type KvHttpServerConfig struct {
	EngineConfig  *EngineConfig // store configuration
	ExposeMetrics bool          // flag to expose metrics
	LogLevel      logrus.Level  // sets log level
	Port          int           // port for http server to listen on
	MetricsPort   int           // ports for expose metrics server to listen on
}

func (kv *KvHttpServer) handleGetRequest(ctx *fiber.Ctx) error {
	key := ctx.Params("key")
	value, err := kv.store.Get(key)

	if err != nil {
		if err == ErrKeyNotFound {
			return ctx.Status(404).SendString(err.Error())
		}
		return ctx.Status(500).SendString(err.Error())
	}

	return ctx.SendString(value)
}

func (kv *KvHttpServer) handleSetRequest(ctx *fiber.Ctx) error {
	key := ctx.Params("key")
	value := string(ctx.Body())

	if len(value) == 0 {
		return ctx.SendStatus(400)
	}

	if err := kv.store.Set(key, value); err != nil {
		return ctx.SendStatus(500)
	}

	return ctx.SendString("OK")
}

func (kv *KvHttpServer) handleDeleteRequest(ctx *fiber.Ctx) error {
	key := ctx.Params("key")

	if err := kv.store.Delete(key); err != nil {
		if err == ErrKeyNotFound {
			return ctx.Status(404).SendString(err.Error())
		}
		return ctx.Status(500).SendString(err.Error())
	}

	return ctx.SendString("OK")
}

func (kv *KvHttpServer) setupEndpoints() {
	kv.server.Get("/keys/:key", kv.handleGetRequest)
	kv.server.Post("/keys/:key", kv.handleSetRequest)
	kv.server.Delete("/keys/:key", kv.handleDeleteRequest)
}

func (kv *KvHttpServer) setupMiddlewares() {
	kv.server.Use(compress.New())
	kv.server.Use(logger.New())
	kv.server.Use(helmet.New())
}

func (kv *KvHttpServer) setupInterruptHandler() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		<-signalChan // block till signal is received
		logrus.Info("Received an interrupt, cleaning up...")
		if err := kv.store.Close(); err != nil {
			logrus.Fatal(err)
		}
		os.Exit(0)
	}()
}

func (kv *KvHttpServer) loadConfigs() error {
	config, err := configuro.NewConfig(
		configuro.WithLoadFromEnvVars("KV"),
		configuro.WithLoadFromConfigFile(ConfigPath, false),
	)

	if err != nil {
		return err
	}

	err = config.Load(kv.config)
	if err != nil {
		return err
	}

	return nil
}

func (kv *KvHttpServer) StartServer() error {
	if err := kv.loadConfigs(); err != nil {
		return err
	}

	engine, err := NewEngine(kv.config.EngineConfig)
	if err != nil {
		return err
	}

	kv.store = engine
	kv.setupMiddlewares()
	kv.setupEndpoints()
	kv.setupInterruptHandler()

	defer kv.store.Close()

	if kv.config.ExposeMetrics {
		go kv.startMetricsServer()
	}

	logrus.Infof("using kv http server config %#v", kv.config)
	logrus.Infof("using engine config %#v", kv.config.EngineConfig)

	logrus.Infof("starting kv http server on port %d", kv.config.Port)
	return kv.server.Listen(fmt.Sprintf(":%d", kv.config.Port))
}

func (kv *KvHttpServer) startMetricsServer() error {
	logrus.Infof("starting kv metrics http server on port %d", kv.config.MetricsPort)

	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(fmt.Sprintf(":%d", kv.config.MetricsPort), nil)
}

func NewHttpServer() (*KvHttpServer, error) {
	path, err := filepath.Abs(fmt.Sprintf("%s/.kv/data", os.Getenv("HOME")))

	if err != nil {
		return nil, err
	}

	return &KvHttpServer{
		server: fiber.New(),
		config: &KvHttpServerConfig{
			EngineConfig: &EngineConfig{
				SegmentMaxSize:             10000,
				SnapshotInterval:           10 * time.Second,
				TolerableSnapshotFailCount: 5,
				CacheSize:                  1000,
				CompactorInterval:          10 * time.Second,
				CompactorWorkerCount:       3,
				SnapshotTTLDuration:        15 * time.Second,
				DataPath:                   path,
			},
			ExposeMetrics: true,
			LogLevel:      logrus.InfoLevel,
			Port:          9998,
			MetricsPort:   9999,
		},
	}, nil
}
