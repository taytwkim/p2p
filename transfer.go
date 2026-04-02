package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

/*
 * Transfer Protocol, stream-based protocol handling file downloads.
 * setupTransferProtocol is called once in node startup.
 * doFetch issues the transfer request, and handleTransferStream is the handler.
 */

const transferProtocol = "/p2pfs/get/1.0.0"

type TransferRequest struct {
	CID string `json:"cid"`
}

type TransferResponse struct {
	Error    string `json:"error,omitempty"`
	Filesize int64  `json:"filesize,omitempty"`
	Filename string `json:"filename,omitempty"`
}

func (n *Node) setupTransferProtocol() {
	n.Host.SetStreamHandler(transferProtocol, n.handleTransferStream)
}

func (n *Node) doFetch(cid string, targetPeerID string) error {
	return n.doFetchWithProgress(cid, targetPeerID, nil, nil)
}

func (n *Node) doFetchWithProgress(cid string, targetPeerID string, progress func(written, total int64), status func(format string, args ...any)) error {
	var target peer.ID
	var err error
	var providerInfo peer.AddrInfo
	var remoteFilename string

	if targetPeerID != "" {
		target, err = peer.Decode(targetPeerID)
		if err != nil {
			return fmt.Errorf("invalid peer id: %v", err)
		}
	} else {
		providers, err := n.DHT.FindProviders(context.Background(), cid, 20)
		if err != nil {
			return fmt.Errorf("failed to query DHT providers: %w", err)
		}
		if len(providers) == 0 {
			return errors.New("no providers known for this CID. Use 'whohas' first")
		}
		validated := false
		for _, candidate := range providers {
			targetAddr := addrInfoToP2PAddr(candidate)
			if targetAddr == "" {
				continue
			}

			hasCID, err := n.doHas(targetAddr, cid)
			if err != nil {
				log.Printf("Skipping provider %s during HAS probe: %v", candidate.ID, err)
				continue
			}
			if !hasCID {
				log.Printf("Skipping stale provider %s for CID %s", candidate.ID, cid)
				continue
			}

			target = candidate.ID
			providerInfo = candidate
			validated = true
			break
		}
		if !validated {
			return errors.New("no live providers confirmed for this CID")
		}
	}

	emitFetchStatus(status, "Fetching %s from %s", cid, target)

	ctx := context.Background() // For transfer, we just use background, but real app might want timeout

	if providerInfo.ID != "" && len(providerInfo.Addrs) > 0 {
		if err := n.Host.Connect(ctx, providerInfo); err != nil {
			emitFetchStatus(status, "Warning: failed to explicitly connect to provider: %v", err)
		}
	}

	s, err := n.Host.NewStream(ctx, target, transferProtocol)
	if err != nil {
		return fmt.Errorf("failed to open transfer stream: %w", err)
	}
	defer s.Close()

	// Send request
	req := TransferRequest{CID: cid}
	encoder := json.NewEncoder(s)
	if err := encoder.Encode(req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// Read response header
	var resp TransferResponse
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&resp); err != nil {
		return fmt.Errorf("failed to read response header: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("remote error: %s", resp.Error)
	}

	emitFetchStatus(status, "Incoming filesize: %d bytes", resp.Filesize)

	filename := safeDownloadFilename(resp.Filename, remoteFilename, cid)

	// Save to a temp file first, then verify the bytes really match the CID we
	// requested before exposing them as a finished local object.
	tempPath := filepath.Join(n.ExportDir, filename+".downloading")
	finalPath := uniqueDownloadPath(filepath.Join(n.ExportDir, filename))

	outFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Read data
	written, err := copyWithProgress(outFile, io.LimitReader(s, resp.Filesize), resp.Filesize, progress)
	outFile.Close()

	if err != nil && err != io.EOF {
		os.Remove(tempPath)
		return fmt.Errorf("transfer failed mid-stream: %w", err)
	}

	if written != resp.Filesize {
		os.Remove(tempPath)
		return fmt.Errorf("incomplete file transfer: got %d, expected %d", written, resp.Filesize)
	}

	computedCID, err := ComputeCID(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to compute CID after download: %w", err)
	}
	if computedCID != cid {
		os.Remove(tempPath)
		return fmt.Errorf("downloaded bytes do not match requested CID: got %s", computedCID)
	}

	// Rename final
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename finalized file: %w", err)
	}

	// Update local files map so we instantly serve it
	n.localFilesLock.Lock()
	n.LocalFiles[cid] = LocalFileRecord{
		CID:      cid,
		Filename: filepath.Base(finalPath),
		Path:     finalPath,
		Size:     resp.Filesize,
	}
	n.localFilesLock.Unlock()

	emitFetchStatus(status, "Successfully downloaded %s as %s", cid, filepath.Base(finalPath))
	return nil
}

func emitFetchStatus(status func(format string, args ...any), format string, args ...any) {
	if status != nil {
		status(format, args...)
		return
	}
	log.Printf(format, args...)
}

func copyWithProgress(dst io.Writer, src io.Reader, total int64, progress func(written, total int64)) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64

	if progress != nil {
		progress(0, total)
	}

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			written += int64(nw)
			if progress != nil {
				progress(written, total)
			}
			if ew != nil {
				return written, ew
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}

		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}

func (n *Node) handleTransferStream(s network.Stream) {
	defer s.Close()

	// Read Request
	var req TransferRequest
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Failed to read transfer request: %v", err)
		return
	}

	encoder := json.NewEncoder(s)

	log.Printf("Received GET request for %s from %s", req.CID, s.Conn().RemotePeer())

	// Check if file exists
	n.localFilesLock.RLock()
	record, exists := n.LocalFiles[req.CID]
	n.localFilesLock.RUnlock()

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

	// Send Response Header
	if err := encoder.Encode(TransferResponse{Filesize: record.Size, Filename: record.Filename}); err != nil {
		return
	}

	// Stream Bytes
	written, err := io.Copy(s, file)
	if err != nil {
		log.Printf("Error sending CID %s: %v", req.CID, err)
	} else {
		log.Printf("Sent %d bytes of CID %s to %s", written, req.CID, s.Conn().RemotePeer())
	}
}

func safeDownloadFilename(primaryName, fallbackName, cid string) string {
	for _, name := range []string{primaryName, fallbackName} {
		if name == "" {
			continue
		}
		if strings.Contains(name, "/") || strings.Contains(name, "\\") {
			// safety check to prevent overwriting outside the export directory
			// don't allow files like ../hello.txt
			continue
		}
		return name
	}
	return cid + ".bin"
}

func addrInfoToP2PAddr(info peer.AddrInfo) string {
	if len(info.Addrs) == 0 {
		return ""
	}
	return fmt.Sprintf("%s/p2p/%s", info.Addrs[0], info.ID)
}

// make sure we don’t overwrite an existing local file.
// If the target path is free, it returns it unchanged.
// If the filename already exists, generate names like name-1.ext, name-2.ext
func uniqueDownloadPath(path string) string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}

	ext := filepath.Ext(path)
	base := strings.TrimSuffix(filepath.Base(path), ext)
	dir := filepath.Dir(path)

	for i := 1; ; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s-%d%s", base, i, ext))
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
}
