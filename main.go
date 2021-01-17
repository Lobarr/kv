package main

import (
	"os"

	"github.com/sirupsen/logrus"

	"kv/core"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stdout)
}

func main() {
	server := core.NewHttpServer()

	if err := server.StartServer(); err != nil {
		logrus.Fatal(err)
	}
}
