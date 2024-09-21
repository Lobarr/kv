package main

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"

	"kv/core"
)

func init() {
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)

	prometheus.Register(collectors.NewGoCollector())
}

func main() {
	server, err := core.NewHttpServer()
	if err != nil {
		logrus.Fatal(err)
	}

	if err := server.StartServer(); err != nil {
		logrus.Fatal(err)
	}
}
