package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// This file defines how peers send manifest and chunk bytes to each other.
// Fetching starts from a manifest CID, then the manifest tells us which chunks
// to download and join back into a normal file.

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

func (n *Node) doFetch(cid string, targetPeerID string) error {
	return n.doFetchWithStatus(cid, targetPeerID, nil)
}

// doFetchWithStatus starts a download from a manifest CID.
// It finds a peer, asks for the manifest bytes, checks that the response is actually a manifest,
// and then hands off to finishChunkedFetch to download the chunks.
func (n *Node) doFetchWithStatus(manifestCID string, targetPeerID string, status func(format string, args ...any)) error {
	// Pick the peer that should provide the manifest.
	// If the user supplied a specific peer, this uses that peer; otherwise it asks the DHT.
	target, providerInfo, err := n.findFetchTarget(manifestCID, targetPeerID)
	if err != nil {
		return err
	}

	emitFetchStatus(status, "Fetching manifest %s from %s", manifestCID, target)

	ctx := context.Background()

	// If findFetchTarget returned an address, connect explicitly before opening the transfer stream.
	if providerInfo.ID != "" && len(providerInfo.Addrs) > 0 {
		if err := n.Host.Connect(ctx, providerInfo); err != nil {
			emitFetchStatus(status, "Warning: failed to explicitly connect to provider: %v", err)
		}
	}

	// Open a stream to the selected peer and ask for the manifest CID.
	s, err := n.Host.NewStream(ctx, target, transferProtocol)
	if err != nil {
		return fmt.Errorf("failed to open transfer stream: %w", err)
	}
	defer s.Close()

	if err := json.NewEncoder(s).Encode(TransferRequest{CID: manifestCID}); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// The response starts with one JSON header line, followed by raw bytes.
	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(s, &resp)
	if err != nil {
		return fmt.Errorf("failed to read response header: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("remote error: %s", resp.Error)
	}

	// Fetch is now manifest-only, so a chunk CID or any other object kind is a user error.
	if resp.Kind != string(ObjectManifest) {
		return fmt.Errorf("expected manifest CID, got %q object", resp.Kind)
	}

	return n.finishChunkedFetch(bodyReader, manifestCID, resp, targetPeerID, status)
}

// finishChunkedFetch turns a downloaded manifest into a final local file.
// It verifies the manifest, downloads the chunks listed inside it, joins those
// chunks in order, and checks that the rebuilt file matches the manifest.
func (n *Node) finishChunkedFetch(r io.Reader, manifestCID string, resp TransferResponse, targetPeerID string, status func(format string, args ...any)) error {
	emitFetchStatus(status, "Fetching manifest %s", manifestCID)

	// Read exactly the manifest bytes promised by the response header.
	manifestBytes, err := io.ReadAll(io.LimitReader(r, resp.Filesize))
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}
	if int64(len(manifestBytes)) != resp.Filesize {
		return fmt.Errorf("incomplete manifest transfer: got %d, expected %d", len(manifestBytes), resp.Filesize)
	}

	// The manifest CID must match the actual manifest bytes we received.
	computedCID, err := ComputeCIDFromBytes(manifestBytes)
	if err != nil {
		return fmt.Errorf("failed to verify manifest CID: %w", err)
	}
	if computedCID != manifestCID {
		return fmt.Errorf("manifest bytes do not match requested CID: got %s", computedCID)
	}

	// Decode the manifest JSON so we can see the filename, final file CID, and chunk list.
	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}
	if manifest.Version != manifestVersion {
		return fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}

	// Save the manifest locally so this peer can serve it later too.
	if err := ensureP2PFSDirs(n.ExportDir); err != nil {
		return err
	}
	if err := os.WriteFile(manifestStoragePath(n.ExportDir, manifestCID), manifestBytes, 0644); err != nil {
		return fmt.Errorf("failed to cache manifest: %w", err)
	}

	emitFetchStatus(status, "Manifest describes %s: %d bytes, %d chunks", manifest.Filename, manifest.FileSize, len(manifest.Chunks))
	// Download every chunk listed by the manifest. This function handles the
	// parallel workers.
	if err := n.fetchManifestChunks(&manifest, targetPeerID, status); err != nil {
		return err
	}

	// Rebuild into a temp file first, so an interrupted download does not leave a
	// half-finished file with the final name.
	tempPath := filepath.Join(n.ExportDir, manifest.Filename+".downloading")
	finalPath := uniqueDownloadPath(filepath.Join(n.ExportDir, safeDownloadFilename(manifest.Filename, manifest.FileCID)))

	outFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp output: %w", err)
	}

	// Join cached chunks in manifest order.
	for _, chunk := range manifest.Chunks {
		chunkFile, err := os.Open(chunkStoragePath(n.ExportDir, chunk.CID))
		if err != nil {
			outFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to open cached chunk %d: %w", chunk.Index, err)
		}
		_, copyErr := io.Copy(outFile, chunkFile)
		chunkFile.Close()
		if copyErr != nil {
			outFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to assemble chunk %d: %w", chunk.Index, copyErr)
		}
	}
	outFile.Close()

	// Verify the rebuilt file against the complete file CID stored in the
	// manifest.
	computedFileCID, err := ComputeCID(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to compute reconstructed file CID: %w", err)
	}
	if computedFileCID != manifest.FileCID {
		os.Remove(tempPath)
		return fmt.Errorf("reconstructed file CID mismatch: got %s, expected %s", computedFileCID, manifest.FileCID)
	}

	// Now it is safe to expose the finished file to the user.
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename reconstructed file: %w", err)
	}

	// Rescan so this peer advertises the new manifest and chunks on future
	// lookups.
	n.updateLocalObjects()
	emitFetchStatus(status, "Successfully reconstructed %s as %s", manifest.FileCID, filepath.Base(finalPath))
	return nil
}

// fetchManifestChunks downloads all chunks from a manifest.
// This is the part that makes chunk downloading parallel: a few worker goroutines pull chunk jobs from
// a shared queue until all chunks have been fetched.
func (n *Node) fetchManifestChunks(manifest *Manifest, targetPeerID string, status func(format string, args ...any)) error {
	if len(manifest.Chunks) == 0 {
		return nil
	}

	// Keep the demo simple: at most four chunks are downloaded at the same time.
	parallelism := 4
	if len(manifest.Chunks) < parallelism {
		parallelism = len(manifest.Chunks)
	}

	// jobs is the queue of chunks to fetch.
	// errCh collects worker errors so the caller can fail the whole download if any chunk fails.
	jobs := make(chan ManifestChunk)
	errCh := make(chan error, len(manifest.Chunks))
	var wg sync.WaitGroup

	// Start worker goroutines.
	// Each worker waits for a chunk job, fetches it,
	// then waits for another one until the jobs channel is closed.
	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range jobs {
				if err := n.fetchOneChunk(chunk, targetPeerID, status); err != nil {
					errCh <- err
				}
			}
		}()
	}

	// Send every chunk to the workers.
	for _, chunk := range manifest.Chunks {
		jobs <- chunk
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	// Return the first chunk error, if any worker reported one.
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchOneChunk downloads one chunk and stores it in the local chunk cache.
// This is a helper used by the parallel workers in fetchManifestChunks.
func (n *Node) fetchOneChunk(chunk ManifestChunk, targetPeerID string, status func(format string, args ...any)) error {
	chunkPath := chunkStoragePath(n.ExportDir, chunk.CID)
	// If we already have this chunk and its bytes still match the CID, reuse it.
	if cachedCID, err := ComputeCID(chunkPath); err == nil && cachedCID == chunk.CID {
		emitFetchStatus(status, "chunk %d cached locally", chunk.Index)
		return nil
	}

	// Download this chunk by its own CID.
	data, resp, providerID, err := n.downloadObjectBytes(chunk.CID, targetPeerID)
	if err != nil {
		return fmt.Errorf("chunk %d fetch failed: %w", chunk.Index, err)
	}
	// The remote peer must send a chunk object, not a manifest.
	if resp.Kind != string(ObjectChunk) {
		return fmt.Errorf("chunk %d expected chunk object, got %q", chunk.Index, resp.Kind)
	}
	// The chunk size in the manifest must match what we received.
	if int64(len(data)) != chunk.Size {
		return fmt.Errorf("chunk %d size mismatch: got %d, expected %d", chunk.Index, len(data), chunk.Size)
	}

	// Write to a temp file first, then rename it into the cache.
	tempPath := chunkPath + ".downloading"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, chunkPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Add the downloaded chunk to LocalObjects so this peer can serve it.
	n.localObjectsLock.Lock()
	n.LocalObjects[chunk.CID] = LocalObjectRecord{
		CID:    chunk.CID,
		Kind:   ObjectChunk,
		Path:   chunkPath,
		Size:   chunk.Size,
		Length: chunk.Size,
	}
	n.localObjectsLock.Unlock()
	// Announce the chunk so other peers can find it through the DHT.
	if err := n.DHT.Provide(n.ctx, chunk.CID, true); err == nil {
		n.providedLock.Lock()
		n.ProvidedCIDs[chunk.CID] = struct{}{}
		n.providedLock.Unlock()
	}

	emitFetchStatus(status, "chunk %d fetched from %s (%d bytes)", chunk.Index, providerID, chunk.Size)
	return nil
}

// downloadObjectBytes downloads one CID-sized object into memory. It is a helper
// for small objects like chunks: it finds a provider, asks for the CID, reads the
// bytes, and verifies that the bytes match the CID requested.
func (n *Node) downloadObjectBytes(cid string, targetPeerID string) ([]byte, TransferResponse, peer.ID, error) {
	// Decide which peer should provide this CID.
	target, providerInfo, err := n.findFetchTarget(cid, targetPeerID)
	if err != nil {
		return nil, TransferResponse{}, "", err
	}

	ctx := context.Background()
	if providerInfo.ID != "" && len(providerInfo.Addrs) > 0 {
		_ = n.Host.Connect(ctx, providerInfo)
	}

	// Ask the chosen peer for this CID.
	s, err := n.Host.NewStream(ctx, target, transferProtocol)
	if err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to open transfer stream: %w", err)
	}
	defer s.Close()

	if err := json.NewEncoder(s).Encode(TransferRequest{CID: cid}); err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to send request: %w", err)
	}

	// Read the response header first, then the raw object bytes.
	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(s, &resp)
	if err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to read response header: %w", err)
	}
	if resp.Error != "" {
		return nil, resp, target, fmt.Errorf("remote error: %s", resp.Error)
	}

	// Read exactly the number of bytes promised by the response header.
	data, err := io.ReadAll(io.LimitReader(bodyReader, resp.Filesize))
	if err != nil {
		return nil, resp, target, err
	}
	if int64(len(data)) != resp.Filesize {
		return nil, resp, target, fmt.Errorf("incomplete object transfer: got %d, expected %d", len(data), resp.Filesize)
	}

	// Verify that the bytes really are the object we asked for.
	computedCID, err := ComputeCIDFromBytes(data)
	if err != nil {
		return nil, resp, target, err
	}
	if computedCID != cid {
		return nil, resp, target, fmt.Errorf("downloaded bytes do not match requested CID: got %s", computedCID)
	}

	return data, resp, target, nil
}

// findFetchTarget chooses which peer to ask for a CID. If the user named a
// specific peer, this helper uses it directly; otherwise it asks the DHT and
// double-checks each candidate with HAS before trusting it.
func (n *Node) findFetchTarget(cid string, targetPeerID string) (peer.ID, peer.AddrInfo, error) {
	// User-specified peer: use it directly.
	if targetPeerID != "" {
		target, err := peer.Decode(targetPeerID)
		if err != nil {
			return "", peer.AddrInfo{}, fmt.Errorf("invalid peer id: %v", err)
		}
		return target, peer.AddrInfo{}, nil
	}

	// No specific peer: ask the DHT who claims to provide this CID.
	providers, err := n.DHT.FindProviders(context.Background(), cid, 20)
	if err != nil {
		return "", peer.AddrInfo{}, fmt.Errorf("failed to query DHT providers: %w", err)
	}
	if len(providers) == 0 {
		return "", peer.AddrInfo{}, errors.New("no providers known for this CID. Use 'whohas' first")
	}

	for _, candidate := range providers {
		targetAddr := addrInfoToP2PAddr(candidate)
		if targetAddr == "" {
			continue
		}

		// Provider records can be stale, so ask the peer directly before using it.
		hasCID, err := n.doHas(targetAddr, cid)
		if err != nil {
			log.Printf("Skipping provider %s during HAS probe: %v", candidate.ID, err)
			continue
		}
		if !hasCID {
			log.Printf("Skipping stale provider %s for CID %s", candidate.ID, cid)
			continue
		}

		return candidate.ID, candidate, nil
	}
	return "", peer.AddrInfo{}, errors.New("no live providers confirmed for this CID")
}

// emitFetchStatus is a tiny helper for progress messages. If the caller gave us
// a status function, send the message there; otherwise write it to the normal
// log.
func emitFetchStatus(status func(format string, args ...any), format string, args ...any) {
	if status != nil {
		status(format, args...)
		return
	}
	log.Printf(format, args...)
}

// handleTransferStream answers incoming GET requests from other peers. It looks
// up the requested CID in LocalObjects, opens the local bytes, and streams either
// a manifest file or a chunk byte range back to the requester.
func (n *Node) handleTransferStream(s network.Stream) {
	defer s.Close()

	// Read the requested CID.
	var req TransferRequest
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Failed to read transfer request: %v", err)
		return
	}

	encoder := json.NewEncoder(s)

	log.Printf("Received GET request for %s from %s", req.CID, s.Conn().RemotePeer())

	// Look up the requested object. It may be a manifest or a chunk.
	n.localObjectsLock.RLock()
	record, exists := n.LocalObjects[req.CID]
	n.localObjectsLock.RUnlock()

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

	// For chunks backed by a larger source file, move to the chunk's starting
	// byte before sending.
	if record.Offset > 0 {
		if _, err := file.Seek(record.Offset, io.SeekStart); err != nil {
			encoder.Encode(TransferResponse{Error: "Internal server error"})
			return
		}
	}

	// Send one metadata line before sending raw bytes.
	if err := writeTransferResponseHeader(s, TransferResponse{Kind: string(record.Kind), Filesize: record.Size, Filename: record.Filename}); err != nil {
		return
	}

	// Send the manifest file or the exact chunk byte range.
	reader := io.Reader(file)
	if record.Kind == ObjectChunk {
		reader = io.LimitReader(file, record.Length)
	}
	written, err := io.Copy(s, reader)
	if err != nil {
		log.Printf("Error sending CID %s: %v", req.CID, err)
	} else {
		log.Printf("Sent %d bytes of CID %s to %s", written, req.CID, s.Conn().RemotePeer())
	}
}

// readTransferResponseHeader is a helper for reading the metadata line at the
// start of a transfer response. It returns a reader positioned at the first byte
// of the actual manifest or chunk content.
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

// writeTransferResponseHeader is the matching helper for the sender side. It
// writes one JSON line before the sender streams the raw manifest or chunk
// bytes.
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

// safeDownloadFilename is a helper that chooses a local output filename. It
// rejects names that try to escape the export directory, such as
// "../hello.txt".
func safeDownloadFilename(primaryName, fallbackCID string) string {
	for _, name := range []string{primaryName, fallbackCID + ".bin"} {
		if name == "" {
			continue
		}
		if filepath.Base(name) != name {
			continue
		}
		if strings.Contains(name, "/") || strings.Contains(name, "\\") {
			continue
		}
		return name
	}
	return "download.bin"
}

// addrInfoToP2PAddr is a small helper for turning libp2p peer info into the
// address string used by doHas.
func addrInfoToP2PAddr(info peer.AddrInfo) string {
	if len(info.Addrs) == 0 {
		return ""
	}
	return fmt.Sprintf("%s/p2p/%s", info.Addrs[0], info.ID)
}

// uniqueDownloadPath is a helper that avoids overwriting an existing file. If
// "name.txt" already exists, it tries "name-1.txt", "name-2.txt", and so on.
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
