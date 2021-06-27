package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"kv/core"
)

func init() {
	prometheus.Register(core.CompressBytesDurationNanoseconds)
	prometheus.Register(core.CompressBytesDurationMilliseconds)
	prometheus.Register(core.UncompressBytesDurationNanoseconds)
	prometheus.Register(core.UncompressBytesDurationMilliseconds)
	prometheus.Register(core.AddLogEntryDurationNanoseconds)
	prometheus.Register(core.AddLogEntryDurationMilliseconds)
	prometheus.Register(core.GetLogEntryDurationNanoseconds)
	prometheus.Register(core.GetLogEntryDurationMilliseconds)
	prometheus.Register(core.CloseDataSegmentDurationNanoseconds)
	prometheus.Register(core.CloseDataSegmentDurationMilliseconds)
	prometheus.Register(core.SegmentEntriesCount)
	prometheus.Register(core.NewDataSegmentDurationNanoseconds)
	prometheus.Register(core.NewDataSegmentDurationMilliseconds)
	prometheus.Register(core.LoadDataSegmentDurationNanoseconds)
	prometheus.Register(core.LoadDataSegmentDurationMilliseconds)
	prometheus.Register(core.SegmentMetadataListAddDurationNanoseconds)
	prometheus.Register(core.SegmentMetadataListAddDurationMilliseconds)
	prometheus.Register(core.SegmentMetadataListRemoveDurationNanoseconds)
	prometheus.Register(core.SegmentMetadataListRemoveDurationMilliseconds)
	prometheus.Register(core.EngineCaptureSnapshotDurationNanoseconds)
	prometheus.Register(core.EngineCaptureSnapshotDurationMilliseconds)
	prometheus.Register(core.EngineLoadSnapshotDurationNanoseconds)
	prometheus.Register(core.EngineLoadSnapshotDurationMilliseconds)
	prometheus.Register(core.EngineRolloverSegmentDurationNanoseconds)
	prometheus.Register(core.EngineRolloverSegmentDurationMilliseconds)
	prometheus.Register(core.EngineSetDurationNanoseconds)
	prometheus.Register(core.EngineSetDurationMilliseconds)
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
