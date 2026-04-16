package main

import (
	"bytes"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// returns a CIDv1 for raw file bytes using a sha2-256 multihash.
func ComputeCID(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return ComputeCIDFromReader(file)
}

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
