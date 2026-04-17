package main

import (
	"bytes"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// returns CID (SHA 256 hash) of a file specified by the file path
func ComputeCID(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return ComputeCIDFromReader(file)
}

// returns CID of an array of bytes
func ComputeCIDFromBytes(data []byte) (string, error) {
	return ComputeCIDFromReader(bytes.NewReader(data))
}

func ComputeCIDFromReader(r io.Reader) (string, error) {
	hash, err := mh.SumStream(r, mh.SHA2_256, -1)
	if err != nil {
		return "", err
	}

	return cid.NewCidV1(cid.Raw, hash).String(), nil
}
