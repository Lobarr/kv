package core

import (
	"bytes"
	"compress/flate"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CompressBytesDurationNanoseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_nanoseconds",
		Help: "how long it took to compress an array of bytes",
	})

	CompressBytesDurationMilliseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_milliseconds",
		Help: "how long it took to compress an array of bytes",
	})

	RawByteSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "raw_byte_sizes",
		Help: "size of payloads being compressed",
	})

	UncompressBytesDurationNanoseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_nanoseconds",
		Help: "how long it took to uncompress an array of bytes",
	})

	UncompressBytesDurationMilliseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_milliseconds",
		Help: "how long it took to uncompress an array of bytes",
	})

	CompressedByteSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compressed_byte_sizes",
		Help: "size of payloads after compression",
	})
)

// compressBytes compresses an input byte array using flate
func compressBytes(rawBytes []byte) ([]byte, error) {
	start := time.Now()
	var compressedBytes bytes.Buffer

	compressor, err := flate.NewWriter(&compressedBytes, flate.BestSpeed)
	if err != nil {
		return nil, err
	}

	if _, err = compressor.Write(rawBytes); err != nil {
		return nil, err
	}

	if err = compressor.Close(); err != nil {
		return nil, err
	}

	RawByteSizes.Observe(float64(len(rawBytes)))
	CompressedByteSizes.Observe(float64(compressedBytes.Len()))
	CompressBytesDurationNanoseconds.Observe(float64(time.Since(start).Nanoseconds()))
	CompressBytesDurationMilliseconds.Observe(float64(time.Since(start).Milliseconds()))

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

	RawByteSizes.Observe(float64(rawBuffer.Len()))
	CompressedByteSizes.Observe(float64(len(compressedBytes)))
	CompressBytesDurationNanoseconds.Observe(float64(time.Since(start).Nanoseconds()))
	CompressBytesDurationMilliseconds.Observe(float64(time.Since(start).Milliseconds()))

	return rawBuffer.Bytes(), nil
}
