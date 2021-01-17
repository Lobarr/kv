package core

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/helmet/v2"
	"github.com/sherifabdlnaby/configuro"
	"github.com/sirupsen/logrus"
)

type KvHttpServer struct {
	server       *fiber.App    // http server instance
	store        Store         // store instance
	engineConfig *EngineConfig // store configuration
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
		<-signalChan
		logrus.Info("Received an interrupt, cleaning up...")
		kv.store.Close()
		return
	}()
}

func (kv *KvHttpServer) loadConfigs() error {
	config, err := configuro.NewConfig(configuro.WithLoadFromEnvVars("KV"))

	if err != nil {
		return err
	}

	err = config.Load(kv.engineConfig)

	if err != nil {
		return err
	}

	return nil
}

func (kv *KvHttpServer) StartServer() error {
	if err := kv.loadConfigs(); err != nil {
		return err
	}

	fmt.Println(kv.engineConfig)

	engine, err := NewEngine(kv.engineConfig)
	if err != nil {
		return err
	}

	kv.store = engine
	kv.setupMiddlewares() 
	kv.setupEndpoints()
	//kv.setupInterruptHandler()
	port := 8080

	defer kv.store.Close()

	logrus.Infof("starting kv http server on port %d", port)

	return kv.server.Listen(fmt.Sprintf(":%d", port)) //TODO: make port configurable
}

func NewHttpServer() *KvHttpServer {
	return &KvHttpServer{
		server: fiber.New(),
		engineConfig: &EngineConfig{
			SegmentMaxSize:             2000,
			SnapshotInterval:           5 * time.Second,
			TolerableSnapshotFailCount: 5,
			CacheSize:                  100,
			CompactorInterval:          15 * time.Second,
			CompactorWorkerCount:       3,
			SnapshotTTLDuration:        15 * time.Second,
		},
	}
}
