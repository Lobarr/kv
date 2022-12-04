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
	prometheus.Register(core.RawByteSizes)
	prometheus.Register(core.CompressedByteSizes)
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
	prometheus.Register(core.EngineLoadDataSegmentDurationNanoseconds)
	prometheus.Register(core.EngineLoadDataSegmentDurationMilliseconds)
	prometheus.Register(core.EngineFindLogEntryByKeyDurationNanoseconds)
	prometheus.Register(core.EngineFindLogEntryByKeyDurationMilliseconds)
	prometheus.Register(core.EngineGetDurationNanoseconds)
	prometheus.Register(core.EngineGetDurationMilliseconds)
	prometheus.Register(core.EngineDeleteDurationNanoseconds)
	prometheus.Register(core.EngineDeleteDurationMilliseconds)
	prometheus.Register(core.EngineCloseDurationNanoseconds)
	prometheus.Register(core.EngineCloseDurationMilliseconds)
	prometheus.Register(core.EngineCompactSegmentsDurationNanoseconds)
	prometheus.Register(core.EngineCompactSegmentsDurationMilliseconds)
	prometheus.Register(core.EngineCompactSnapshotsDurationNanoseconds)
	prometheus.Register(core.EngineCompactSnapshotsDurationMilliseconds)
	prometheus.Register(core.EngineSegmentsCompactionCount)
	prometheus.Register(core.EngineSnapshotsCompactionCount)
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
