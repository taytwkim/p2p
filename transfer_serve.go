package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// This file contains the inbound transfer protocol: register the stream
// handler, answer GET requests, and handle the small response framing helpers.

const transferProtocol = "/p2pfs/get/1.0.0"

type TransferRequest struct {
	CID string `json:"cid"`
}

type TransferResponse struct {
	Error    string `json:"error,omitempty"`
	Kind     string `json:"kind,omitempty"`
	Filesize int64  `json:"filesize,omitempty"`
	Filename string `json:"filename,omitempty"`
}

func (n *Node) setupTransferProtocol() {
	n.Host.SetStreamHandler(transferProtocol, n.handleTransferStream)
}

// handleTransferStream answers incoming GET requests from other peers. It looks
// up the requested CID in ServedObjects, opens the local bytes, and streams
// either a manifest file or a piece byte range back to the requester.
func (n *Node) handleTransferStream(s network.Stream) {
	defer s.Close()

	var req TransferRequest
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Failed to read transfer request: %v", err)
		return
	}

	encoder := json.NewEncoder(s)
	log.Printf("Received GET request for %s from %s", req.CID, s.Conn().RemotePeer())

	n.stateLock.RLock()
	record, exists := n.ServedObjects[req.CID]
	n.stateLock.RUnlock()

	if !exists {
		encoder.Encode(TransferResponse{Error: "File not found"})
		return
	}

	file, err := os.Open(record.Path)
	if err != nil {
		encoder.Encode(TransferResponse{Error: "Internal server error"})
		return
	}
	defer file.Close()

	if record.Offset > 0 {
		if _, err := file.Seek(record.Offset, io.SeekStart); err != nil {
			encoder.Encode(TransferResponse{Error: "Internal server error"})
			return
		}
	}

	if err := writeTransferResponseHeader(s, TransferResponse{
		Kind:     string(record.Kind),
		Filesize: record.Size,
		Filename: record.Filename,
	}); err != nil {
		return
	}

	reader := io.Reader(file)
	if record.Kind == ObjectPiece {
		reader = io.LimitReader(file, record.Length)
	}
	written, err := io.Copy(s, reader)
	if err != nil {
		log.Printf("Error sending CID %s: %v", req.CID, err)
	} else {
		log.Printf("Sent %d bytes of CID %s to %s", written, req.CID, s.Conn().RemotePeer())
	}
}

// readTransferResponseHeader reads the metadata line at the start of a
// transfer response and returns a reader positioned at the first body byte.
func readTransferResponseHeader(r io.Reader, resp *TransferResponse) (io.Reader, error) {
	buffered := bufio.NewReader(r)
	line, err := buffered.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(line, resp); err != nil {
		return nil, err
	}
	return buffered, nil
}

// writeTransferResponseHeader writes one JSON line before the raw manifest or
// piece bytes.
func writeTransferResponseHeader(w io.Writer, resp TransferResponse) error {
	line, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := w.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

// addrInfoToP2PAddr turns libp2p peer info into the address string used by
// doHas.
func addrInfoToP2PAddr(info peer.AddrInfo) string {
	if len(info.Addrs) == 0 {
		return ""
	}
	return fmt.Sprintf("%s/p2p/%s", info.Addrs[0], info.ID)
}
