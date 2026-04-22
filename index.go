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
 * Index Protocol is a protocol allowing peers to verify what files a target peer is serving.
 * 		- setupIndexProtocol is called once in node startup.
 * 		- doX issues a request, and handleIndexStream is the handler.
 *
 * The Index Protocol supports three types of requests, specified by the "Op" field.
 * 		- LIST lists manifests served by a peer.
 * 		- HAS confirms if peer has CID X (both manifests and pieces).
 * 		- AVAILABILITY reports which pieces a peer has for a manifest CID.
 */

const indexProtocol = "/p2pfs/index/1.0.0"

type IndexRequest struct {
	Op          string `json:"op"`
	CID         string `json:"cid,omitempty"`
	ManifestCID string `json:"manifestCid,omitempty"`
}

type IndexFile struct {
	CID         string `json:"cid"`
	Kind        string `json:"kind,omitempty"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ManifestCID string `json:"manifestCid,omitempty"`
	PieceCount  int    `json:"pieceCount,omitempty"`
}

type IndexResponse struct {
	Files        []IndexFile `json:"files,omitempty"`
	Has          bool        `json:"has,omitempty"`
	Availability []bool      `json:"availability,omitempty"`
	Error        string      `json:"error,omitempty"`
}

// called once at node startup
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

// "For this manifest CID, which of its pieces do you currently have?"
// e.g., {true, false, true, true} means that the peer has pieces 0, 2, and 3.
func (n *Node) doAvailability(info peer.AddrInfo, manifestCID string) ([]bool, error) {
	ctx := context.Background()

	if err := n.Host.Connect(ctx, info); err != nil {
		log.Printf("Warning: failed to connect to %s explicitly: %v", info.ID, err)
	}

	s, err := n.Host.NewStream(ctx, info.ID, indexProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to open index stream: %w", err)
	}
	defer s.Close()

	req := IndexRequest{Op: "AVAILABILITY", ManifestCID: manifestCID}
	if err := json.NewEncoder(s).Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send AVAILABILITY request: %w", err)
	}

	var resp IndexResponse
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read AVAILABILITY response: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("remote error: %s", resp.Error)
	}

	return resp.Availability, nil
}

// Given a string like: /ip4/127.0.0.1/tcp/4001/p2p/12D3KooW...
// parse it into a struct
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
		n.stateLock.RLock()
		var files []IndexFile
		for _, f := range n.CompleteFiles {
			files = append(files, IndexFile{
				CID:         f.ManifestCID,
				Kind:        string(ObjectManifest),
				Filename:    f.Manifest.Filename,
				Size:        f.Manifest.FileSize,
				ManifestCID: f.ManifestCID,
				PieceCount:  len(f.Manifest.Pieces),
			})
		}
		n.stateLock.RUnlock()

		encoder.Encode(IndexResponse{Files: files})

	case "HAS":
		n.stateLock.RLock()
		_, exists := n.ServedObjects[req.CID]
		n.stateLock.RUnlock()

		encoder.Encode(IndexResponse{Has: exists})

	case "AVAILABILITY":
		if req.ManifestCID == "" {
			encoder.Encode(IndexResponse{Error: "manifestCid is required"})
			return
		}

		availability, err := n.availabilityForManifest(req.ManifestCID)
		if err != nil {
			encoder.Encode(IndexResponse{Error: err.Error()})
			return
		}
		encoder.Encode(IndexResponse{Availability: availability})

	default:
		encoder.Encode(IndexResponse{Error: "Unknown operation"})
	}
}

// helper for computing a bitfield like {true, false, true, true}
func (n *Node) availabilityForManifest(manifestCID string) ([]bool, error) {
	n.stateLock.RLock()
	record, exists := n.ServedObjects[manifestCID]
	if !exists || record.Kind != ObjectManifest || record.Manifest == nil {
		n.stateLock.RUnlock()
		return nil, fmt.Errorf("manifest not found")
	}

	pieces := append([]ManifestPiece(nil), record.Manifest.Pieces...)
	availability := make([]bool, len(pieces))
	for i, piece := range pieces {
		if localPiece, ok := n.ServedObjects[piece.CID]; ok && localPiece.Kind == ObjectPiece {
			availability[i] = true
		}
	}
	n.stateLock.RUnlock()

	return availability, nil
}
