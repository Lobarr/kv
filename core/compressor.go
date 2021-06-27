package core

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	compressBytesDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_nanoseconds",
		Help: "how long it took to compress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	compressBytesDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_milliseconds",
		Help: "how long it took to compress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	uncompressBytesDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_nanoseconds",
		Help: "how long it took to uncompress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	uncompressBytesDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_milliseconds",
		Help: "how long it took to uncompress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})
)

// compressBytes compresses an input byte array using zlib
func compressBytes(rawBytes []byte) ([]byte, error) {
	start := time.Now()
	var compressedBytes bytes.Buffer
	compressor := zlib.NewWriter(&compressedBytes)

	_, err := compressor.Write(rawBytes)
	if err != nil {
		return nil, err
	}

	if err = compressor.Close(); err != nil {
		return nil, err
	}

	compressBytesDurationNanoseconds.With(prometheus.Labels{
		"raw_bytes_size":        fmt.Sprint(len(rawBytes)),
		"compressed_bytes_size": fmt.Sprint(compressedBytes.Len()),
	}).Observe(float64(time.Since(start).Nanoseconds()))

	compressBytesDurationMilliseconds.With(prometheus.Labels{
		"raw_bytes_size":        fmt.Sprint(len(rawBytes)),
		"compressed_bytes_size": fmt.Sprint(compressedBytes.Len()),
	}).Observe(float64(time.Since(start).Microseconds()))

	return compressedBytes.Bytes(), nil
}

//uncompresssLogEntryBytes uncompresses a bytes array using zlib
func uncompressBytes(compressedBytes []byte) ([]byte, error) {
	start := time.Now()
	rawBuffer := bytes.NewBuffer(make([]byte, 0))

	uncompressor, err := zlib.NewReader(bytes.NewReader(compressedBytes))
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(rawBuffer, uncompressor)
	if err != nil {
		return nil, err
	}

	if err = uncompressor.Close(); err != nil {
		return nil, err
	}

	uncompressBytesDurationNanoseconds.WithLabelValues(
		fmt.Sprint(len(rawBuffer.Bytes())),
		fmt.Sprint(len(compressedBytes)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	uncompressBytesDurationMilliseconds.WithLabelValues(
		fmt.Sprint(len(rawBuffer.Bytes())),
		fmt.Sprint(len(compressedBytes)),
	).Observe(
		float64(time.Since(start).Microseconds()),
	)

	return rawBuffer.Bytes(), nil
}
