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
	Items []KVItem `json:"items"`
}

type BatchGetResponse struct {
	Items []KVItem `json:"items"`
}

type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
}

func (s *HttpServer) batchSet(c *fiber.Ctx) error {
	var req BatchSetRequest
	if err := c.BodyParser(&req); err != nil {
		s.engine.logger.Errorf("error parsing batch set request: %v", err)
		return err
	}

	if err := s.engine.BatchSet(req.Items); err != nil {
		return err
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

	values, err := s.engine.BatchGet(req.Keys)
	if err != nil {
		return err
	}

	res := BatchGetResponse{Items: make([]KVItem, 0, len(values))}
	for _, key := range req.Keys {
		if value, ok := values[key]; ok {
			res.Items = append(res.Items, KVItem{Key: key, Value: value})
		}
	}

	return c.JSON(res)
}

func (s *HttpServer) batchDelete(c *fiber.Ctx) error {
	var req BatchDeleteRequest
	if err := c.BodyParser(&req); err != nil {
		s.engine.logger.Errorf("error parsing batch delete request: %v", err)
		return err
	}

	if err := s.engine.BatchDelete(req.Keys); err != nil {
		return err
	}

	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *HttpServer) Start(addr string) error {
	return s.app.Listen(addr)
}

func (s *HttpServer) Shutdown() error {
	return s.app.Shutdown()
}
