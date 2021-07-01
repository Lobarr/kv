package core

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CompressBytesDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_nanoseconds",
		Help: "how long it took to compress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	CompressBytesDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_milliseconds",
		Help: "how long it took to compress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	UncompressBytesDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_nanoseconds",
		Help: "how long it took to uncompress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})

	UncompressBytesDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_milliseconds",
		Help: "how long it took to uncompress an array of bytes",
	}, []string{"raw_bytes_size", "compressed_bytes_size"})
)

// compressBytes compresses an input byte array using flate
func compressBytes(rawBytes []byte) ([]byte, error) {
	start := time.Now()
	var compressedBytes bytes.Buffer

	compressor, err := flate.NewWriter(&compressedBytes, flate.BestSpeed)
	if err != nil {
		return nil, err
	}

	_, err = compressor.Write(rawBytes)
	if err != nil {
		return nil, err
	}

	if err = compressor.Close(); err != nil {
		return nil, err
	}

	CompressBytesDurationNanoseconds.WithLabelValues(
		fmt.Sprint(len(rawBytes)),
		fmt.Sprint(compressedBytes.Len()),
	).Observe(float64(time.Since(start).Nanoseconds()))

	CompressBytesDurationMilliseconds.WithLabelValues(
		fmt.Sprint(len(rawBytes)),
		fmt.Sprint(compressedBytes.Len()),
	).Observe(float64(time.Since(start).Milliseconds()))

	return compressedBytes.Bytes(), nil
}

//uncompresssBytes uncompresses a bytes array using flate
func uncompressBytes(compressedBytes []byte) ([]byte, error) {
	start := time.Now()
	rawBuffer := bytes.NewBuffer(make([]byte, 0))
	uncompressor := flate.NewReader(bytes.NewReader(compressedBytes))

	_, err := io.Copy(rawBuffer, uncompressor)
	if err != nil {
		return nil, err
	}

	if err = uncompressor.Close(); err != nil {
		return nil, err
	}

	UncompressBytesDurationNanoseconds.WithLabelValues(
		fmt.Sprint(len(rawBuffer.Bytes())),
		fmt.Sprint(len(compressedBytes)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	UncompressBytesDurationMilliseconds.WithLabelValues(
		fmt.Sprint(len(rawBuffer.Bytes())),
		fmt.Sprint(len(compressedBytes)),
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)

	return rawBuffer.Bytes(), nil
}
