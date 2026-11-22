package core

import (
	"context"
	"kv/protos"
)

type GrpcServer struct {
	protos.UnimplementedKVServer
	engine *Engine
}

func NewGrpcServer(engine *Engine) *GrpcServer {
	return &GrpcServer{
		engine: engine,
	}
}

func (s *GrpcServer) Set(ctx context.Context, req *protos.SetRequest) (*protos.SetResponse, error) {
	err := s.engine.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &protos.SetResponse{}, nil
}

func (s *GrpcServer) Get(ctx context.Context, req *protos.GetRequest) (*protos.GetResponse, error) {
	value, err := s.engine.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &protos.GetResponse{Value: value}, nil
}

func (s *GrpcServer) Delete(ctx context.Context, req *protos.DeleteRequest) (*protos.DeleteResponse, error) {
	err := s.engine.Delete(req.Key)
	if err != nil {
		return nil, err
	}
	return &protos.DeleteResponse{}, nil
}

func (s *GrpcServer) BatchSet(ctx context.Context, req *protos.BatchSetRequest) (*protos.BatchSetResponse, error) {
	items := make([]KVItem, len(req.Requests))
	for i, r := range req.Requests {
		items[i] = KVItem{Key: r.Key, Value: r.Value}
	}
	if err := s.engine.BatchSet(items); err != nil {
		return nil, err
	}
	return &protos.BatchSetResponse{}, nil
}

func (s *GrpcServer) BatchGet(ctx context.Context, req *protos.BatchGetRequest) (*protos.BatchGetResponse, error) {
	keys := make([]string, len(req.Requests))
	for i, r := range req.Requests {
		keys[i] = r.Key
	}
	values, err := s.engine.BatchGet(keys)
	if err != nil {
		return nil, err
	}
	responses := make([]*protos.GetResponse, len(req.Requests))
	for i, key := range keys {
		val, ok := values[key]
		if !ok {
			return nil, ErrKeyNotFound
		}
		responses[i] = &protos.GetResponse{Value: val}
	}
	return &protos.BatchGetResponse{Responses: responses}, nil
}

func (s *GrpcServer) BatchDelete(ctx context.Context, req *protos.BatchDeleteRequest) (*protos.BatchDeleteResponse, error) {
	keys := make([]string, len(req.Requests))
	for i, r := range req.Requests {
		keys[i] = r.Key
	}
	if err := s.engine.BatchDelete(keys); err != nil {
		return nil, err
	}
	return &protos.BatchDeleteResponse{}, nil
}
