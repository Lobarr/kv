package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"kv/core"
)

func init() {
	// metrics from core/compressor.go
	prometheus.Register(core.CompressBytesDurationNanoseconds)
	prometheus.Register(core.CompressBytesDurationMilliseconds)
	prometheus.Register(core.RawByteSizes)
	prometheus.Register(core.UncompressBytesDurationNanoseconds)
	prometheus.Register(core.UncompressBytesDurationMilliseconds)
	prometheus.Register(core.CompressedByteSizes)

	// metrics from core/data_segment.go
	prometheus.Register(core.DataSegmentOperationDurationNanoseconds)
	prometheus.Register(core.DataSegmentOperationDurationMilliseconds)
	prometheus.Register(core.DataSegmentFileSizes)
	prometheus.Register(core.DataSegmentLogEntryKeySizes)
	prometheus.Register(core.DataSegmentLogEntryValueSizes)
	prometheus.Register(core.DataSegmentLogEntryCount)
	prometheus.Register(core.DataSegmentLogEntrySizes)
	prometheus.Register(core.DataSegmentCompressedLogEntrySizes)

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
