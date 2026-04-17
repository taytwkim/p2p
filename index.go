package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

/*
 * Index Protocol is a request protocol allowing peers to manually verify what files a target peer is serving.
 * 		- setupIndexProtocol is called once in node startup.
 * 		- doList issues a request, and handleIndexStream is the handler.
 *
 * The Index Protocol supports TWO types of requests, specified by the "Op" field.
 * 		- LIST lists all files served by a peer.
 * 		- HAS confirms if peer has CID X, and is used internally before a fetch.
 */

const indexProtocol = "/p2pfs/index/1.0.0"

type IndexRequest struct {
	Op  string `json:"op"` // "LIST" or "HAS"
	CID string `json:"cid,omitempty"`
}

type IndexFile struct {
	CID         string `json:"cid"`
	Kind        string `json:"kind,omitempty"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ManifestCID string `json:"manifestCid,omitempty"`
	ChunkCount  int    `json:"chunkCount,omitempty"`
}

type IndexResponse struct {
	Files []IndexFile `json:"files,omitempty"`
	Has   bool        `json:"has,omitempty"`
	Error string      `json:"error,omitempty"`
}

func (n *Node) setupIndexProtocol() {
	n.Host.SetStreamHandler(indexProtocol, n.handleIndexStream)
}

func (n *Node) doList(targetAddr string) ([]IndexFile, error) {
	info, err := addrInfoFromTarget(targetAddr)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	// Connect first if not already connected
	if err := n.Host.Connect(ctx, *info); err != nil {
		log.Printf("Warning: failed to connect to %s explicitly: %v", info.ID, err)
	}

	s, err := n.Host.NewStream(ctx, info.ID, indexProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to open index stream: %w", err)
	}
	defer s.Close()

	req := IndexRequest{Op: "LIST"}
	if err := json.NewEncoder(s).Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send LIST request: %w", err)
	}

	var resp IndexResponse
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read LIST response: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("remote error: %s", resp.Error)
	}

	return resp.Files, nil
}

func (n *Node) doHas(targetAddr string, cid string) (bool, error) {
	info, err := addrInfoFromTarget(targetAddr)
	if err != nil {
		return false, err
	}

	ctx := context.Background()

	if err := n.Host.Connect(ctx, *info); err != nil {
		log.Printf("Warning: failed to connect to %s explicitly: %v", info.ID, err)
	}

	s, err := n.Host.NewStream(ctx, info.ID, indexProtocol)
	if err != nil {
		return false, fmt.Errorf("failed to open index stream: %w", err)
	}
	defer s.Close()

	req := IndexRequest{Op: "HAS", CID: cid}
	if err := json.NewEncoder(s).Encode(req); err != nil {
		return false, fmt.Errorf("failed to send HAS request: %w", err)
	}

	var resp IndexResponse
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		return false, fmt.Errorf("failed to read HAS response: %w", err)
	}

	if resp.Error != "" {
		return false, fmt.Errorf("remote error: %s", resp.Error)
	}

	return resp.Has, nil
}

func addrInfoFromTarget(targetAddr string) (*peer.AddrInfo, error) {
	maddr, err := multiaddr.NewMultiaddr(targetAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid multiaddr: %w", err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("invalid addr info: %w", err)
	}
	return info, nil
}

func (n *Node) handleIndexStream(s network.Stream) {
	defer s.Close()

	var req IndexRequest
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Failed to read index request: %v", err)
		return
	}

	encoder := json.NewEncoder(s)

	switch req.Op {
	case "LIST":
		log.Printf("Received LIST request from %s", s.Conn().RemotePeer())
		n.localObjectsLock.RLock()
		var files []IndexFile
		for _, f := range n.LocalObjects {
			if f.Kind != ObjectManifest {
				continue
			}
			files = append(files, IndexFile{
				CID:         f.CID,
				Kind:        string(f.Kind),
				Filename:    f.Filename,
				Size:        f.Manifest.FileSize,
				ManifestCID: f.ManifestCID,
				ChunkCount:  f.ChunkCount,
			})
		}
		n.localObjectsLock.RUnlock()

		encoder.Encode(IndexResponse{Files: files})

	case "HAS":
		n.localObjectsLock.RLock()
		_, exists := n.LocalObjects[req.CID]
		n.localObjectsLock.RUnlock()

		encoder.Encode(IndexResponse{Has: exists})

	default:
		encoder.Encode(IndexResponse{Error: "Unknown operation"})
	}
}
