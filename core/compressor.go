package core

import (
	"bytes"
	"compress/zlib"
	"io"
)

// compressBytes compresses an input byte array using zlib
func compressBytes(rawBytes []byte) ([]byte, error) {
	var compressedBytes bytes.Buffer
	compressor := zlib.NewWriter(&compressedBytes)

	_, err := compressor.Write(rawBytes)
	if err != nil {
		return nil, err
	}

	if err = compressor.Close(); err != nil {
		return nil, err
	}

	return compressedBytes.Bytes(), nil
}

//uncompresssLogEntryBytes uncompresses a bytes array using zlib
func uncompressBytes(compressedBytes []byte) ([]byte, error) {
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

	return rawBuffer.Bytes(), nil
}
