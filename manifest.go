package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	manifestVersion = 1

	// This intentionally small size makes early demos easy to inspect:
	// "AAAA\nBBBB\nCCCC\n" becomes three chunks.
	defaultChunkSize int64 = 5
)

type Manifest struct {
	Version   int             `json:"version"`
	Filename  string          `json:"filename"`
	FileSize  int64           `json:"fileSize"`
	FileCID   string          `json:"fileCid"`
	ChunkSize int64           `json:"chunkSize"`
	Chunks    []ManifestChunk `json:"chunks"`
}

type ManifestChunk struct {
	Index  int    `json:"index"`
	CID    string `json:"cid"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

func BuildManifest(path, filename string, chunkSize int64) (*Manifest, []byte, string, error) {
	if chunkSize <= 0 {
		return nil, nil, "", fmt.Errorf("chunk size must be positive")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, "", err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, nil, "", err
	}

	fileCID, err := ComputeCID(path)
	if err != nil {
		return nil, nil, "", err
	}

	manifest := &Manifest{
		Version:   manifestVersion,
		Filename:  filename,
		FileSize:  info.Size(),
		FileCID:   fileCID,
		ChunkSize: chunkSize,
	}

	buf := make([]byte, chunkSize)
	var offset int64
	for index := 0; ; index++ {
		n, readErr := io.ReadFull(file, buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return nil, nil, "", readErr
		}

		chunkBytes := buf[:n]
		chunkCID, err := ComputeCIDFromBytes(chunkBytes)
		if err != nil {
			return nil, nil, "", err
		}

		manifest.Chunks = append(manifest.Chunks, ManifestChunk{
			Index:  index,
			CID:    chunkCID,
			Size:   int64(n),
			Offset: offset,
		})
		offset += int64(n)

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, nil, "", err
	}

	manifestCID, err := ComputeCIDFromBytes(manifestBytes)
	if err != nil {
		return nil, nil, "", err
	}

	return manifest, manifestBytes, manifestCID, nil
}
