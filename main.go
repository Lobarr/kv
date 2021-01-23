package main

import (
	"os"

	"github.com/sirupsen/logrus"

	"kv/core"
)

func init() {
  env, ok := os.LookupEnv("ENV") 

  if ok && (env == "development" || env == "dev") {
    logrus.SetLevel(logrus.DebugLevel)
  } else {
    logrus.SetLevel(logrus.InfoLevel)
  }

 	logrus.SetOutput(os.Stdout)
}

func main() {
	server := core.NewHttpServer()

	if err := server.StartServer(); err != nil {
		logrus.Fatal(err)
	}
}
