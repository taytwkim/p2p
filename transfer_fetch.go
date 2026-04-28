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

	emitFetchHighlight(status, "Fetching manifest %s from %s", manifestCID, target)

	ctx := context.Background()
	if providerInfo.ID != "" && len(providerInfo.Addrs) > 0 {
		if err := n.Host.Connect(ctx, providerInfo); err != nil {
			emitFetchHighlight(status, "Warning: failed to explicitly connect to provider: %v", err)
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
	emitFetchHighlight(status, "Fetching manifest %s", manifestCID)

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

	emitFetchHighlight(status, "Manifest describes %s: %d bytes, %d pieces", manifest.Filename, manifest.FileSize, len(manifest.Pieces))
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
	emitFetchHighlight(status, "Successfully reconstructed %s as %s", manifest.FileCID, filepath.Base(finalPath))
	return nil
}

/*
 * fetchMissingPieces is the main piece-download scheduler for one manifest.
 *
 * Each round does five things in order:
 * 		1. identify which pieces are still missing after checking the local cache,
 * 		2. ask participating peers which of those pieces they can currently serve,
 * 		3. rank the missing pieces rarest-first,
 * 		4. fan the work out across a bounded number of workers, and
 * 		5. if every provider for some piece is currently choking us, wait and retry.
 *
 * The loop repeats because choking is treated as temporary. A round may make
 * partial progress on some pieces, then come back later for the ones that were
 * blocked by provider cooldowns.
 *
 * A “round” here means one full pass over the current missingPieces list.
 */
func (n *Node) fetchMissingPieces(manifestCID string, manifest *Manifest, status func(format string, args ...any)) error {
	allPieces := removeDuplicatePieces(manifest.Pieces)

	for {
		// First, trust the local cache before the network. Anything already
		// verified on disk drops out of the round.
		missingPieces := n.findMissingPieces(manifestCID, allPieces, status)
		if len(missingPieces) == 0 {
			return nil
		}

		// Ask currently known participants which of the remaining pieces they
		// can serve right now.
		pieceSources, err := n.findPeersForPieces(manifestCID, manifest, status)
		if err != nil {
			return err
		}

		// if everything is still missing, we know we have zero verified pieces.
		// In that case, bootstrap with one random piece first.
		// Once we have at least one piece, fall back to the normal rarest-first order.
		hasAnyVerifiedPiece := len(missingPieces) != len(allPieces)
		missingPieces = schedulePiecesForRound(missingPieces, pieceSources, hasAnyVerifiedPiece)

		// Every piece in this round must have at least one reported provider.
		// If not, the manifest is incomplete from the swarm's point of view.
		for _, piece := range missingPieces {
			if len(pieceSources[piece.CID]) == 0 {
				return fmt.Errorf("no swarm peer reported piece %d", piece.Index)
			}
		}

		// Use a bounded worker count so one fetch round does not explode into
		// unbounded concurrent piece requests.
		numWorkers := currentSystemConfig().MaxDownloadWorkers
		if len(missingPieces) < numWorkers {
			numWorkers = len(missingPieces)
		}

		jobs := make(chan ManifestPiece)
		errCh := make(chan error, len(missingPieces))
		var wg sync.WaitGroup

		// Start workers
		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// `range jobs` does:
				//   1. receive one piece from the shared jobs channel,
				//   2. fetch it,
				//   3. repeat until `jobs` is closed and drained.
				for piece := range jobs {
					// The worker chooses a provider at execution time, not at queue
					// construction time, so it can respect the latest cooldowns.
					if err := n.fetchPieceFromProviders(manifestCID, piece, pieceSources[piece.CID], status); err != nil {
						errCh <- err
					}
				}
			}()
		}

		// This loop defines the work for one round: send every still-missing
		// piece into the shared queue once. Multiple workers may consume these
		// jobs in parallel, but the sends happen here one piece at a time.
		for _, piece := range missingPieces {
			jobs <- piece
		}

		// No more pieces will be sent in this round. Workers finish whatever
		// they already received, then exit their `range jobs` loop.
		close(jobs)

		// Wait for every worker launched above to finish before we inspect the
		// round's errors. After this point, nothing will send into errCh again.
		wg.Wait()
		close(errCh)

		// If some pieces in this round were skipped because we were choked by all providers (choke err),
		// we can retry when the cooldowns expire. Any other error aborts the fetch.
		retryBecauseChoked := false
		for err := range errCh {
			if err == nil {
				continue
			}
			if errors.Is(err, errAllProvidersChoked) {
				retryBecauseChoked = true
				continue
			}
			return err
		}

		if !retryBecauseChoked {
			// A clean round does not necessarily mean we are done anymore.
			// The random-first bootstrap round intentionally fetches only one piece,
			// so we loop back, rescan the local cache, and schedule the remaining work.
			continue
		}

		// If choking was the only blocker, pause until the earliest relevant
		// cooldown expires, then run another scheduling round.
		delay := n.nextChokeRetryDelay(manifestCID, missingPieces, pieceSources)
		emitFetchHighlight(status, "All current providers for at least one piece are choking us; retrying in %s", delay.Round(time.Second))
		time.Sleep(delay)
	}
}

// findMissingPieces returns the manifest pieces that are not already cached locally.
// If a cached piece is valid, it marks that piece as available right away.
func (n *Node) findMissingPieces(manifestCID string, pieces []ManifestPiece, status func(format string, args ...any)) []ManifestPiece {
	missing := make([]ManifestPiece, 0, len(pieces))
	for _, piece := range pieces {
		piecePath := pieceStoragePath(n.ExportDir, piece.CID)
		if cachedCID, err := ComputeCID(piecePath); err == nil && cachedCID == piece.CID {
			n.markPieceAvailableFrom(manifestCID, piece, "Local cache")
			logFetchDetail("piece %d cached locally", piece.Index)
			continue
		}
		missing = append(missing, piece)
	}
	return missing
}

// findPeersForPieces asks peers participating in the manifest for their availability bitfields
// and records which peers can serve each piece in the manifest.
func (n *Node) findPeersForPieces(manifestCID string, manifest *Manifest, status func(format string, args ...any)) (map[string][]peer.ID, error) {
	participants, err := n.findPeersWhoHasManifest(manifestCID)
	if err != nil {
		return nil, err
	}

	emitFetchHighlight(status, "Discovered %d swarm peer(s) for manifest %s", len(participants), manifestCID)

	pieceSources := make(map[string][]peer.ID, len(manifest.Pieces))
	for _, info := range participants {
		// Register the peer in per-manifest state before selection so it can be
		// treated as an unknown candidate even before its first successful piece.
		n.ensurePeerStateExists(manifestCID, info.ID)
		availability, err := n.doAvailability(info, manifestCID)
		if err != nil {
			logFetchDetail("Skipping %s: availability failed: %v", info.ID, err)
			continue
		}
		if len(availability) != len(manifest.Pieces) {
			logFetchDetail("Skipping %s: availability length %d does not match %d pieces", info.ID, len(availability), len(manifest.Pieces))
			continue
		}
		for i, hasPiece := range availability {
			if hasPiece {
				piece := manifest.Pieces[i]
				pieceSources[piece.CID] = append(pieceSources[piece.CID], info.ID)
			}
		}
	}

	logFetchDetail("Piece availability:")
	for _, piece := range manifest.Pieces {
		peers := pieceSources[piece.CID]
		if len(peers) == 0 {
			logFetchDetail("  piece %d: none", piece.Index)
			continue
		}
		logFetchDetail("  piece %d: %s", piece.Index, formatPeerIDs(peers))
	}

	return pieceSources, nil
}

// findPeersWhoHasManifest returns peers that currently claim this manifest and
// still confirm that they have it when probed directly.
func (n *Node) findPeersWhoHasManifest(manifestCID string) ([]peer.AddrInfo, error) {
	providers, err := n.DHT.FindProviders(context.Background(), manifestCID, currentSystemConfig().ProviderQueryLimit)
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
		n.markPieceAvailableFrom(manifestCID, piece, "Local cache")
		logFetchDetail("piece %d cached locally", piece.Index)
		return nil
	}

	n.markPieceInFlight(manifestCID, piece)
	defer n.clearPieceInFlight(manifestCID, piece)

	start := time.Now()
	data, resp, providerID, err := n.fetchObjectFromPeer(piece.CID, source)
	if err != nil {
		if errors.Is(err, errPeerChoked) {
			return errPeerChoked
		}
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

	n.markPieceAvailableFrom(manifestCID, piece, providerID.String())
	n.recordPeerDownloadSample(manifestCID, providerID, piece.Size, time.Since(start))
	logFetchDetail("piece %d fetched from %s (%d bytes)", piece.Index, providerID, piece.Size)
	return nil
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
		if resp.Error == transferErrorChoked {
			return nil, resp, target, errPeerChoked
		}
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
	providers, err := n.DHT.FindProviders(context.Background(), manifestCID, currentSystemConfig().ProviderQueryLimit)
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

// emitFetchHighlight writes a high-level fetch progress line to the normal log
// and, when a caller asked for live status, forwards it to that callback too.
func emitFetchHighlight(status func(format string, args ...any), format string, args ...any) {
	log.Printf(format, args...)
	if status != nil {
		status(format, args...)
	}
}

// logFetchDetail records fetch internals in the normal log without sending
// every line back to the interactive caller or RPC client.
func logFetchDetail(format string, args ...any) {
	log.Printf(format, args...)
}
