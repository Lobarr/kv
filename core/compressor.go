package core

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CompressBytesDurationNanoseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_ns",
		Help: "how long it took to compress an array of bytes",
	})

	CompressBytesDurationMilliseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compress_bytes_duration_ms",
		Help: "how long it took to compress an array of bytes",
	})

	RawByteSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "raw_byte_sizes",
		Help: "size of payloads being compressed",
	})

	UncompressBytesDurationNanoseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_ns",
		Help: "how long it took to uncompress an array of bytes",
	})

	UncompressBytesDurationMilliseconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "uncompress_bytes_duration_ms",
		Help: "how long it took to uncompress an array of bytes",
	})

	CompressedByteSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "compressed_byte_sizes",
		Help: "size of payloads after compression",
	})

	// Pools to reuse expensive objects
	flateWriterPool sync.Pool
	bufferPool      sync.Pool
)

func init() {
	prometheus.Register(CompressBytesDurationNanoseconds)
	prometheus.Register(CompressBytesDurationMilliseconds)
	prometheus.Register(RawByteSizes)
	prometheus.Register(UncompressBytesDurationNanoseconds)
	prometheus.Register(UncompressBytesDurationMilliseconds)
	prometheus.Register(CompressedByteSizes)

	flateWriterPool = sync.Pool{
		New: func() interface{} {
			w, _ := flate.NewWriter(nil, flate.BestSpeed)
			return w
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
}

// compressBytes compresses an input byte array using flate
func compressBytes(rawBytes []byte) ([]byte, error) {
	start := time.Now()

	// Get buffer from pool
	compressedBytes := bufferPool.Get().(*bytes.Buffer)
	compressedBytes.Reset()
	defer bufferPool.Put(compressedBytes)

	// Get writer from pool
	compressor := flateWriterPool.Get().(*flate.Writer)
	compressor.Reset(compressedBytes)
	defer flateWriterPool.Put(compressor)

	if _, err := compressor.Write(rawBytes); err != nil {
		return nil, err
	}

	if err := compressor.Close(); err != nil {
		return nil, err
	}

	// Make a copy regarding of buffer escape analysis, usually safer to return copy for byte slices
	// OR usage might allow direct buffer bytes if lifecycle is managed.
	// For safety, let's copy the result because the buffer is put back to pool.
	result := make([]byte, compressedBytes.Len())
	copy(result, compressedBytes.Bytes())

	RawByteSizes.Observe(float64(len(rawBytes)))
	CompressedByteSizes.Observe(float64(len(result)))
	CompressBytesDurationNanoseconds.Observe(float64(time.Since(start).Nanoseconds()))
	CompressBytesDurationMilliseconds.Observe(float64(time.Since(start).Milliseconds()))

	return result, nil
}

// uncompressBytes uncompresses a bytes array using flate
func uncompressBytes(compressedBytes []byte) ([]byte, error) {
	start := time.Now()

	// Get buffer from pool
	rawBuffer := bufferPool.Get().(*bytes.Buffer)
	rawBuffer.Reset()
	defer bufferPool.Put(rawBuffer)

	uncompressor := flate.NewReader(bytes.NewReader(compressedBytes))
	// flate.NewReader documents it might return an interface that needs Close.
	// But standard flate reader is just an io.ReadCloser implementation.
	// Resetting a reader is tricky, usually we just create new one for reader as it is cheap compared to writer.

	_, err := io.Copy(rawBuffer, uncompressor)
	if err != nil {
		return nil, err
	}

	if err = uncompressor.Close(); err != nil {
		return nil, err
	}

	// Copy result
	result := make([]byte, rawBuffer.Len())
	copy(result, rawBuffer.Bytes())

	RawByteSizes.Observe(float64(len(result)))
	CompressedByteSizes.Observe(float64(len(compressedBytes)))
	CompressBytesDurationNanoseconds.Observe(float64(time.Since(start).Nanoseconds()))
	CompressBytesDurationMilliseconds.Observe(float64(time.Since(start).Milliseconds()))

	return result, nil
}
