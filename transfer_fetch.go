package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

/*
 * This file contains the outbound file-download flow.
 *
 * A download always starts from a manifest CID, not from a piece CID.
 *
 * High-level flow:
 *
 * 1. doFetch / doFetchWithStatus
 *    - Start a download for one manifest CID.
 *    - Pick a peer that still has the manifest.
 *    - Open a transfer stream and request the manifest bytes.
 *
 * 2. fetchFile
 *    - Read and verify the manifest bytes.
 *    - Cache the manifest locally.
 *    - Start DownloadState tracking for this file.
 *    - Ask fetchMissingPieces to download any pieces we do not already have.
 *
 * 3. fetchMissingPieces
 *    - Look at the manifest's piece list.
 *    - Skip any pieces that are already cached locally.
 *    - Ask swarm peers which pieces they have.
 *    - Assign each missing piece to one peer and download the missing pieces in parallel.
 *    - When more than one peer has the same piece, the current code picks
 *      `peers[piece.Index % len(peers)]` so pieces are spread across the
 *      available peers instead of always choosing the first one.
 *
 * 4. fetchPieceFromPeer
 *    - Request one specific piece from one specific peer.
 *    - Verify the returned bytes.
 *    - Write the piece into the local piece cache.
 *    - Mark that piece as available in DownloadState.
 *
 * 5. fetchFile
 *    - After all pieces are present, reopen the cached pieces in manifest order.
 *    - Join them into one temporary output file.
 *    - Verify the reconstructed file CID against the manifest's FileCID.
 *    - Rename the temp file into its final filename.
 *    - Clear DownloadState and rescan local files so the finished file becomes
 *      a normal complete local file.
 */

const (
	maxNumDownloadWorkers = 4
	peerDownloadRateAlpha = 0.8
)

// doFetch downloads a file starting from its manifest CID.
// It is the simple entrypoint used when the caller does not need progress updates.
func (n *Node) doFetch(manifestCID string) error {
	return n.doFetchWithStatus(manifestCID, nil)
}

// doFetchWithStatus starts a manifest-based download and reports progress through status.
// It fetches the manifest first, then hands off to the piece download and reconstruction flow.
func (n *Node) doFetchWithStatus(manifestCID string, status func(format string, args ...any)) error {
	target, providerInfo, err := n.choosePeerForManifest(manifestCID)
	if err != nil {
		return err
	}

	emitFetchStatus(status, "Fetching manifest %s from %s", manifestCID, target)

	ctx := context.Background()
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

	if err := json.NewEncoder(s).Encode(TransferRequest{CID: manifestCID}); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(s, &resp)
	if err != nil {
		return fmt.Errorf("failed to read response header: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("remote error: %s", resp.Error)
	}
	if resp.Kind != string(ObjectManifest) {
		return fmt.Errorf("expected manifest CID, got %q object", resp.Kind)
	}

	return n.fetchFile(bodyReader, manifestCID, resp, status)
}

// fetchFile verifies a downloaded manifest, fetches its pieces,
// joins them in order, and writes the final file to the export directory.
func (n *Node) fetchFile(r io.Reader, manifestCID string, resp TransferResponse, status func(format string, args ...any)) error {
	emitFetchStatus(status, "Fetching manifest %s", manifestCID)

	manifestBytes, err := io.ReadAll(io.LimitReader(r, resp.Filesize))
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}
	if int64(len(manifestBytes)) != resp.Filesize {
		return fmt.Errorf("incomplete manifest transfer: got %d, expected %d", len(manifestBytes), resp.Filesize)
	}

	computedCID, err := ComputeCIDFromBytes(manifestBytes)
	if err != nil {
		return fmt.Errorf("failed to verify manifest CID: %w", err)
	}
	if computedCID != manifestCID {
		return fmt.Errorf("manifest bytes do not match requested CID: got %s", computedCID)
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}
	if manifest.Version != manifestVersion {
		return fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}

	if err := ensureTinyTorrentDirs(n.ExportDir); err != nil {
		return err
	}
	manifestPath := manifestStoragePath(n.ExportDir, manifestCID)
	if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
		return fmt.Errorf("failed to cache manifest: %w", err)
	}
	n.startDownloadState(manifestCID, &manifest, manifestPath, int64(len(manifestBytes)))

	emitFetchStatus(status, "Manifest describes %s: %d bytes, %d pieces", manifest.Filename, manifest.FileSize, len(manifest.Pieces))
	if err := n.fetchMissingPieces(manifestCID, &manifest, status); err != nil {
		return err
	}

	tempPath := filepath.Join(n.ExportDir, manifest.Filename+".downloading")
	finalPath := uniqueDownloadPath(filepath.Join(n.ExportDir, safeDownloadFilename(manifest.Filename, manifest.FileCID)))

	outFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp output: %w", err)
	}

	for _, piece := range manifest.Pieces {
		pieceFile, err := os.Open(pieceStoragePath(n.ExportDir, piece.CID))
		if err != nil {
			outFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to open cached piece %d: %w", piece.Index, err)
		}
		_, copyErr := io.Copy(outFile, pieceFile)
		pieceFile.Close()
		if copyErr != nil {
			outFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("failed to assemble piece %d: %w", piece.Index, copyErr)
		}
	}
	outFile.Close()

	computedFileCID, err := ComputeCID(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to compute reconstructed file CID: %w", err)
	}
	if computedFileCID != manifest.FileCID {
		os.Remove(tempPath)
		return fmt.Errorf("reconstructed file CID mismatch: got %s, expected %s", computedFileCID, manifest.FileCID)
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename reconstructed file: %w", err)
	}

	n.clearDownloadState(manifestCID)
	n.updateLocalObjects()
	emitFetchStatus(status, "Successfully reconstructed %s as %s", manifest.FileCID, filepath.Base(finalPath))
	return nil
}

// fetchMissingPieces downloads the pieces we still need for one manifest.
// It first decides which pieces are missing, then spreads those downloads across workers.
func (n *Node) fetchMissingPieces(manifestCID string, manifest *Manifest, status func(format string, args ...any)) error {
	missingPieces := n.findMissingPieces(manifestCID, removeDuplicatePieces(manifest.Pieces), status)
	if len(missingPieces) == 0 {
		return nil
	}

	pieceSources, err := n.findPeersForPieces(manifestCID, manifest, status)
	if err != nil {
		return err
	}

	// Selection policy:
	// 1. order work by rarity, 2. let a worker choose the source peer at fetch time.
	missingPieces = rankPiecesRarestFirst(missingPieces, pieceSources)
	for _, piece := range missingPieces {
		if len(pieceSources[piece.CID]) == 0 {
			return fmt.Errorf("no swarm peer reported piece %d", piece.Index)
		}
	}

	numWorkers := maxNumDownloadWorkers
	if len(missingPieces) < numWorkers {
		numWorkers = len(missingPieces)
	}

	jobs := make(chan ManifestPiece)
	errCh := make(chan error, len(missingPieces))
	var wg sync.WaitGroup

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for piece := range jobs {
				source, err := n.choosePeerForPiece(manifestCID, piece, pieceSources[piece.CID])
				if err != nil {
					errCh <- err
					continue
				}
				if err := n.fetchPieceFromPeer(manifestCID, piece, source, status); err != nil {
					errCh <- err
				}
			}
		}()
	}

	for _, piece := range missingPieces {
		jobs <- piece
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// findMissingPieces returns the manifest pieces that are not already cached locally.
// If a cached piece is valid, it marks that piece as available right away.
func (n *Node) findMissingPieces(manifestCID string, pieces []ManifestPiece, status func(format string, args ...any)) []ManifestPiece {
	missing := make([]ManifestPiece, 0, len(pieces))
	for _, piece := range pieces {
		piecePath := pieceStoragePath(n.ExportDir, piece.CID)
		if cachedCID, err := ComputeCID(piecePath); err == nil && cachedCID == piece.CID {
			n.markPieceAvailable(manifestCID, piece)
			emitFetchStatus(status, "piece %d cached locally", piece.Index)
			continue
		}
		missing = append(missing, piece)
	}
	return missing
}

// rankPiecesRarestFirst returns a new slice ordered by rarity.
// If there is a tie, by order in which the pieces appear in the manifest
func rankPiecesRarestFirst(pieces []ManifestPiece, pieceSources map[string][]peer.ID) []ManifestPiece {
	ranked := append([]ManifestPiece(nil), pieces...)
	sort.SliceStable(ranked, func(i, j int) bool {
		leftProviders := len(pieceSources[ranked[i].CID])
		rightProviders := len(pieceSources[ranked[j].CID])
		if leftProviders != rightProviders {
			return leftProviders < rightProviders
		}
		return ranked[i].Index < ranked[j].Index
	})
	return ranked
}

// For each piece, choose a peer to download from.
// Unknown peers (SamplesDown == 0) get first priority
// otherwise we choose the peer with the highest observed download rate for this manifest.
func (n *Node) choosePeerForPiece(manifestCID string, piece ManifestPiece, providers []peer.ID) (peer.ID, error) {
	if len(providers) == 0 {
		return "", fmt.Errorf("no swarm peer reported piece %d", piece.Index)
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	state := n.DownloadState[manifestCID]
	if state == nil {
		return "", fmt.Errorf("download state missing for manifest %s", manifestCID)
	}
	if state.PeerStats == nil {
		state.PeerStats = make(map[peer.ID]*PeerState)
	}

	var bestPeer peer.ID
	bestRate := -1.0
	for _, provider := range providers {
		peerState, exists := state.PeerStats[provider]
		if !exists {
			peerState = &PeerState{}
			state.PeerStats[provider] = peerState
		}
		if peerState.SamplesDown == 0 {
			return provider, nil
		}
		if peerState.DownloadRate > bestRate {
			bestRate = peerState.DownloadRate
			bestPeer = provider
		}
	}
	if bestPeer == "" {
		return providers[0], nil
	}
	return bestPeer, nil
}

// findPeersForPieces asks peers participating in the manifest for their availability bitfields
// and records which peers can serve each piece in the manifest.
func (n *Node) findPeersForPieces(manifestCID string, manifest *Manifest, status func(format string, args ...any)) (map[string][]peer.ID, error) {
	participants, err := n.findPeersWhoHasManifest(manifestCID)
	if err != nil {
		return nil, err
	}

	emitFetchStatus(status, "Discovered %d swarm peer(s) for manifest %s", len(participants), manifestCID)

	pieceSources := make(map[string][]peer.ID, len(manifest.Pieces))
	for _, info := range participants {
		// Register the peer in per-manifest state before selection so it can be
		// treated as an unknown candidate even before its first successful piece.
		n.ensurePeerStateExists(manifestCID, info.ID)
		availability, err := n.doAvailability(info, manifestCID)
		if err != nil {
			emitFetchStatus(status, "Skipping %s: availability failed: %v", info.ID, err)
			continue
		}
		if len(availability) != len(manifest.Pieces) {
			emitFetchStatus(status, "Skipping %s: availability length %d does not match %d pieces", info.ID, len(availability), len(manifest.Pieces))
			continue
		}
		for i, hasPiece := range availability {
			if hasPiece {
				piece := manifest.Pieces[i]
				pieceSources[piece.CID] = append(pieceSources[piece.CID], info.ID)
			}
		}
	}

	emitFetchStatus(status, "Piece availability:")
	for _, piece := range manifest.Pieces {
		peers := pieceSources[piece.CID]
		if len(peers) == 0 {
			emitFetchStatus(status, "  piece %d: none", piece.Index)
			continue
		}
		emitFetchStatus(status, "  piece %d: %s", piece.Index, formatPeerIDs(peers))
	}

	return pieceSources, nil
}

// findPeersWhoHasManifest returns peers that currently claim this manifest and
// still confirm that they have it when probed directly.
func (n *Node) findPeersWhoHasManifest(manifestCID string) ([]peer.AddrInfo, error) {
	providers, err := n.DHT.FindProviders(context.Background(), manifestCID, 20)
	if err != nil {
		return nil, fmt.Errorf("failed to query DHT swarm participants: %w", err)
	}
	providers = filterSelfProviderCandidates(providers, n.selfPeerID())

	var participants []peer.AddrInfo
	for _, candidate := range providers {
		targetAddr := addrInfoToP2PAddr(candidate)
		if targetAddr == "" {
			continue
		}
		hasManifest, err := n.doHas(targetAddr, manifestCID)
		if err != nil {
			log.Printf("Skipping swarm peer %s during HAS probe: %v", candidate.ID, err)
			continue
		}
		if !hasManifest {
			log.Printf("Skipping stale swarm peer %s for manifest %s", candidate.ID, manifestCID)
			continue
		}
		participants = append(participants, candidate)
	}
	if len(participants) == 0 {
		return nil, errors.New("no live swarm peers confirmed for this manifest")
	}
	return participants, nil
}

// removeDuplicatePieces removes repeated piece CIDs while preserving order.
// This keeps the worker queue from downloading the same piece more than once.
// We get duplicate pieces when two pieces of a file has the exact same contents (e.g., "AAAA\n")
func removeDuplicatePieces(pieces []ManifestPiece) []ManifestPiece {
	unique := make([]ManifestPiece, 0, len(pieces))
	seen := make(map[string]struct{}, len(pieces))
	for _, piece := range pieces {
		if _, ok := seen[piece.CID]; ok {
			continue
		}
		seen[piece.CID] = struct{}{}
		unique = append(unique, piece)
	}
	return unique
}

// fetchPieceFromPeer downloads one piece from one peer and stores it in the local cache.
// After the piece is written successfully, it marks that piece as available in the download state.
func (n *Node) fetchPieceFromPeer(manifestCID string, piece ManifestPiece, source peer.ID, status func(format string, args ...any)) error {
	piecePath := pieceStoragePath(n.ExportDir, piece.CID)
	if cachedCID, err := ComputeCID(piecePath); err == nil && cachedCID == piece.CID {
		n.markPieceAvailable(manifestCID, piece)
		emitFetchStatus(status, "piece %d cached locally", piece.Index)
		return nil
	}

	start := time.Now()
	data, resp, providerID, err := n.fetchObjectFromPeer(piece.CID, source)
	if err != nil {
		return fmt.Errorf("piece %d fetch failed: %w", piece.Index, err)
	}
	if resp.Kind != string(ObjectPiece) {
		return fmt.Errorf("piece %d expected piece object, got %q", piece.Index, resp.Kind)
	}
	if int64(len(data)) != piece.Size {
		return fmt.Errorf("piece %d size mismatch: got %d, expected %d", piece.Index, len(data), piece.Size)
	}

	tempPath := piecePath + ".downloading"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, piecePath); err != nil {
		os.Remove(tempPath)
		return err
	}

	n.markPieceAvailable(manifestCID, piece)
	n.recordPeerDownloadSample(manifestCID, providerID, piece.Size, time.Since(start))
	emitFetchStatus(status, "piece %d fetched from %s (%d bytes)", piece.Index, providerID, piece.Size)
	return nil
}

// recordPeerDownloadSample updates one peer's per-manifest download estimate.
// We turn one piece transfer into a bytes/sec sample, then smooth it by computing a
// moving average so later peer choices are not dominated by a single noisy transfer.
// new_estimate = alpha * new_sample + (1 - alpha) * old_estimate
func (n *Node) recordPeerDownloadSample(manifestCID string, peerID peer.ID, bytes int64, elapsed time.Duration) {
	if peerID == "" || bytes <= 0 {
		return
	}

	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}
	sampleRate := float64(bytes) / seconds

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	state := n.DownloadState[manifestCID]
	if state == nil {
		return
	}
	if state.PeerStats == nil {
		state.PeerStats = make(map[peer.ID]*PeerState)
	}

	peerState, exists := state.PeerStats[peerID]
	if !exists {
		peerState = &PeerState{}
		state.PeerStats[peerID] = peerState
	}

	if peerState.SamplesDown == 0 {
		peerState.DownloadRate = sampleRate
	} else {
		peerState.DownloadRate = peerDownloadRateAlpha*sampleRate + (1-peerDownloadRateAlpha)*peerState.DownloadRate
	}
	peerState.SamplesDown++
}

// fetchObjectFromPeer requests one CID directly from a specific peer and reads it fully into memory.
// It also verifies that the returned bytes hash back to the CID we asked for.
func (n *Node) fetchObjectFromPeer(cid string, target peer.ID) ([]byte, TransferResponse, peer.ID, error) {
	ctx := context.Background()

	s, err := n.Host.NewStream(ctx, target, transferProtocol)
	if err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to open transfer stream: %w", err)
	}
	defer s.Close()

	if err := json.NewEncoder(s).Encode(TransferRequest{CID: cid}); err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to send request: %w", err)
	}

	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(s, &resp)
	if err != nil {
		return nil, TransferResponse{}, target, fmt.Errorf("failed to read response header: %w", err)
	}
	if resp.Error != "" {
		return nil, resp, target, fmt.Errorf("remote error: %s", resp.Error)
	}

	data, err := io.ReadAll(io.LimitReader(bodyReader, resp.Filesize))
	if err != nil {
		return nil, resp, target, err
	}
	if int64(len(data)) != resp.Filesize {
		return nil, resp, target, fmt.Errorf("incomplete object transfer: got %d, expected %d", len(data), resp.Filesize)
	}

	computedCID, err := ComputeCIDFromBytes(data)
	if err != nil {
		return nil, resp, target, err
	}
	if computedCID != cid {
		return nil, resp, target, fmt.Errorf("downloaded bytes do not match requested CID: got %s", computedCID)
	}

	return data, resp, target, nil
}

// choosePeerForManifest chooses which peer to ask for the manifest itself.
// If the user did not name a peer, it looks up swarm participants and probes them with HAS first.
func (n *Node) choosePeerForManifest(manifestCID string) (peer.ID, peer.AddrInfo, error) {
	providers, err := n.DHT.FindProviders(context.Background(), manifestCID, 20)
	if err != nil {
		return "", peer.AddrInfo{}, fmt.Errorf("failed to query DHT swarm participants: %w", err)
	}
	providers = filterSelfProviderCandidates(providers, n.selfPeerID())
	if len(providers) == 0 {
		return "", peer.AddrInfo{}, errors.New("no swarm participants known for this manifest. Use 'whohas' first")
	}

	for _, candidate := range providers {
		targetAddr := addrInfoToP2PAddr(candidate)
		if targetAddr == "" {
			continue
		}

		hasCID, err := n.doHas(targetAddr, manifestCID)
		if err != nil {
			log.Printf("Skipping swarm peer %s during HAS probe: %v", candidate.ID, err)
			continue
		}
		if !hasCID {
			log.Printf("Skipping stale swarm peer %s for manifest %s", candidate.ID, manifestCID)
			continue
		}

		return candidate.ID, candidate, nil
	}
	return "", peer.AddrInfo{}, errors.New("no live swarm participants confirmed for this manifest")
}

func (n *Node) selfPeerID() peer.ID {
	if n == nil || n.Host == nil {
		return ""
	}
	return n.Host.ID()
}

func filterSelfProviderCandidates(providers []peer.AddrInfo, self peer.ID) []peer.AddrInfo {
	if self == "" {
		return providers
	}

	filtered := providers[:0]
	for _, provider := range providers {
		if provider.ID == self {
			continue
		}
		filtered = append(filtered, provider)
	}
	return filtered
}

// safeDownloadFilename picks a safe local filename for the reconstructed file.
// It rejects path-like names so a manifest cannot escape the export directory.
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

// formatPeerIDs joins peer IDs into one printable string for status output.
func formatPeerIDs(peers []peer.ID) string {
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, p.String())
	}
	return strings.Join(parts, ", ")
}

// uniqueDownloadPath avoids overwriting an existing file in the export directory.
// If the base name already exists, it adds a numeric suffix.
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
