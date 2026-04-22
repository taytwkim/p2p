package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	manifestVersion        = 1
	defaultPieceSize int64 = 5
)

// This is how a manifest of a file is organized.
//
// {
//   "version": 1,
//   "filename": "<original filename>",
//   "fileSize": <total file size in bytes>,
//   "fileCid": "<CID of the complete original file>",
//   "pieceSize": <target piece size in bytes>,
//   "pieces": [
//     {
//       "index": 0,
//       "cid": "<CID of piece 0 bytes>",
//       "size": <actual piece 0 size in bytes>,
//       "offset": 0
//     },
//     {
//       "index": 1,
//       "cid": "<CID of piece 1 bytes>",
//       "size": <actual piece 1 size in bytes>,
//       "offset": <byte offset where piece 1 begins>
//     },
//     {
//       "index": 2,
//       "cid": "<CID of piece 2 bytes>",
//       "size": <actual piece 2 size in bytes>,
//       "offset": <byte offset where piece 2 begins>
//     },
// 	   ...
//   ]
// }

type Manifest struct {
	Version   int             `json:"version"`
	Filename  string          `json:"filename"`  // the original filename we want to reconstruct.
	FileSize  int64           `json:"fileSize"`  // total size of the original file in bytes.
	FileCID   string          `json:"fileCid"`   // CID of the complete original file.
	PieceSize int64           `json:"pieceSize"` // target piece size used when splitting the file.
	Pieces    []ManifestPiece `json:"pieces"`    // ordered list of piece records.
}

type ManifestPiece struct {
	Index  int    `json:"index"`  // piece's position in the file.
	CID    string `json:"cid"`    // CID of this piece's bytes.
	Size   int64  `json:"size"`   // size of this piece in bytes (equals pieceSize, except the last piece may be smaller).
	Offset int64  `json:"offset"` // Where this piece starts in the original file.
}

func BuildManifest(path, filename string, pieceSize int64) (*Manifest, []byte, string, error) {
	if pieceSize <= 0 {
		return nil, nil, "", fmt.Errorf("piece size must be positive")
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
		PieceSize: pieceSize,
	}

	buf := make([]byte, pieceSize)
	var offset int64
	for index := 0; ; index++ {
		n, readErr := io.ReadFull(file, buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return nil, nil, "", readErr
		}

		pieceBytes := buf[:n]
		pieceCID, err := ComputeCIDFromBytes(pieceBytes)
		if err != nil {
			return nil, nil, "", err
		}

		manifest.Pieces = append(manifest.Pieces, ManifestPiece{
			Index:  index,
			CID:    pieceCID,
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
