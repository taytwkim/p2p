package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	manifestVersion        = 1
	defaultChunkSize int64 = 5
)

// This is how a manifest of a file is organized.
//
// {
//   "version": 1,
//   "filename": "<original filename>",
//   "fileSize": <total file size in bytes>,
//   "fileCid": "<CID of the complete original file>",
//   "chunkSize": <target chunk size in bytes>,
//   "chunks": [
//     {
//       "index": 0,
//       "cid": "<CID of chunk 0 bytes>",
//       "size": <actual chunk 0 size in bytes>,
//       "offset": 0
//     },
//     {
//       "index": 1,
//       "cid": "<CID of chunk 1 bytes>",
//       "size": <actual chunk 1 size in bytes>,
//       "offset": <byte offset where chunk 1 begins>
//     },
//     {
//       "index": 2,
//       "cid": "<CID of chunk 2 bytes>",
//       "size": <actual chunk 2 size in bytes>,
//       "offset": <byte offset where chunk 2 begins>
//     },
// 	   ...
//   ]
// }

type Manifest struct {
	Version   int             `json:"version"`
	Filename  string          `json:"filename"`  // the original filename we want to reconstruct.
	FileSize  int64           `json:"fileSize"`  // total size of the original file in bytes.
	FileCID   string          `json:"fileCid"`   // CID of the complete original file.
	ChunkSize int64           `json:"chunkSize"` // target chunk size used when splitting the file.
	Chunks    []ManifestChunk `json:"chunks"`    // ordered list of chunk records.
}

type ManifestChunk struct {
	Index  int    `json:"index"`  // chunk’s position in the file.
	CID    string `json:"cid"`    // CID of this chunk’s bytes.
	Size   int64  `json:"size"`   // size of this chunk in bytes (equals chunkSize, except the last chunk may be smaller).
	Offset int64  `json:"offset"` // Where this chunk starts in the original file.
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
