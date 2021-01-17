package main

import (
	"kv/core"
)

func NewStore(config *core.EngineConfig) (core.Store, error) {
	return core.NewEngine(config)
}
