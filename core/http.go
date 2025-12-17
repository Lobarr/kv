package core

import (
	"github.com/gofiber/fiber/v2"
)

type HttpServer struct {
	engine *Engine
	app    *fiber.App
}

func NewHttpServer(engine *Engine) *HttpServer {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	// app.Use(logger.New())

	server := &HttpServer{
		engine: engine,
		app:    app,
	}

	server.setupRoutes()

	return server
}

func (s *HttpServer) setupRoutes() {
	s.app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})
	s.app.Post("/batch/set", s.batchSet)
	s.app.Post("/batch/get", s.batchGet)
	s.app.Post("/batch/delete", s.batchDelete)
}

type BatchSetRequest struct {
	Items []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"items"`
}

type BatchGetResponse struct {
	Items []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"items"`
}

type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
}

func (s *HttpServer) batchSet(c *fiber.Ctx) error {
	// s.engine.logger.Infof("received batch set request: %s", string(c.Body()))
	var req BatchSetRequest
	if err := c.BodyParser(&req); err != nil {
		s.engine.logger.Errorf("error parsing batch set request: %v", err)
		return err
	}

	for _, item := range req.Items {
		if err := s.engine.Set(item.Key, item.Value); err != nil {
			return err
		}
	}

	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *HttpServer) batchGet(c *fiber.Ctx) error {
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := c.BodyParser(&req); err != nil {
		return err
	}

	res := BatchGetResponse{}
	for _, key := range req.Keys {
		value, err := s.engine.Get(key)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			return err
		}
		res.Items = append(res.Items, struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{Key: key, Value: value})
	}

	return c.JSON(res)
}

func (s *HttpServer) batchDelete(c *fiber.Ctx) error {
	// s.engine.logger.Infof("received batch delete request: %s", string(c.Body()))
	var req BatchDeleteRequest
	if err := c.BodyParser(&req); err != nil {
		s.engine.logger.Errorf("error parsing batch delete request: %v", err)
		return err
	}

	for _, key := range req.Keys {
		if err := s.engine.Delete(key); err != nil {
			return err
		}
	}

	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *HttpServer) Start(addr string) error {
	return s.app.Listen(addr)
}

func (s *HttpServer) Shutdown() error {
	return s.app.Shutdown()
}
