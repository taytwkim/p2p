package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	errPeerChoked         = errors.New("peer is choking us")
	errAllProvidersChoked = errors.New("all providers are currently choking us")
)

// rankPiecesRarestFirst returns a new slice ordered by rarity.
// If there is a tie, by order in which the pieces appear in the manifest.
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

// schedulePiecesForRound decides how this download round should start.
//
// If we already have at least one verified piece locally, we keep the normal
// rarest-first behavior.
//
// If we have zero verified pieces, we do a one-piece bootstrap round first.
// That gives us one complete piece quickly, and then the next round falls back
// to normal rarest-first scheduling.
func schedulePiecesForRound(pieces []ManifestPiece, pieceSources map[string][]peer.ID, hasAnyVerifiedPiece bool) []ManifestPiece {
	if hasAnyVerifiedPiece || len(pieces) <= 1 {
		return rankPiecesRarestFirst(pieces, pieceSources)
	}

	// When we have nothing yet, choose one missing piece at random so we can
	// finish a complete piece before starting the full parallel rarest-first flow.
	return []ManifestPiece{pieces[rand.Intn(len(pieces))]}
}

// choosePeerForPiece picks one provider for one piece for this manifest.
// Unknown peers get first priority; otherwise we choose the best measured peer
// that is not currently cooling down because it recently choked us.
func (n *Node) choosePeerForPiece(manifestCID string, piece ManifestPiece, providers []peer.ID) (peer.ID, error) {
	if len(providers) == 0 {
		return "", fmt.Errorf("no swarm peer reported piece %d", piece.Index)
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	if n.ManifestPeerState == nil {
		n.ManifestPeerState = make(map[string]map[peer.ID]*PeerState)
	}
	peerStates, exists := n.ManifestPeerState[manifestCID]
	if !exists {
		peerStates = make(map[peer.ID]*PeerState)
		n.ManifestPeerState[manifestCID] = peerStates
	}

	now := time.Now()
	var bestPeer peer.ID
	bestRate := -1.0
	for _, provider := range providers {
		peerState, exists := peerStates[provider]
		if !exists {
			peerState = &PeerState{Choked: true}
			peerStates[provider] = peerState
		}
		if !peerState.ChokedUntil.IsZero() && now.Before(peerState.ChokedUntil) {
			logFetchDetail("piece %d: skipping provider %s until %s", piece.Index, provider, peerState.ChokedUntil.Format(time.RFC3339))
			continue
		}
		if !peerState.ChokedUntil.IsZero() && !now.Before(peerState.ChokedUntil) {
			peerState.RemoteChokesUs = false
			peerState.ChokedUntil = time.Time{}
			logFetchDetail("piece %d: provider %s cooldown expired", piece.Index, provider)
		}
		if peerState.SamplesDown == 0 {
			logFetchDetail("piece %d: choosing unmeasured provider %s", piece.Index, provider)
			return provider, nil
		}
		if peerState.DownloadRate > bestRate {
			bestRate = peerState.DownloadRate
			bestPeer = provider
		}
	}
	if bestPeer == "" {
		logFetchDetail("piece %d: every known provider is currently cooling down after choking us", piece.Index)
		return "", errAllProvidersChoked
	}
	logFetchDetail("piece %d: choosing fastest available provider %s at %.1f bytes/sec", piece.Index, bestPeer, bestRate)
	return bestPeer, nil
}

// fetchPieceFromProviders tries the available providers for one piece until one
// succeeds or all of them are temporarily unusable because they are choking us.
func (n *Node) fetchPieceFromProviders(manifestCID string, piece ManifestPiece, providers []peer.ID, status func(format string, args ...any)) error {
	remaining := append([]peer.ID(nil), providers...)
	for len(remaining) > 0 {
		source, err := n.choosePeerForPiece(manifestCID, piece, remaining)
		if err != nil {
			if errors.Is(err, errAllProvidersChoked) {
				return errAllProvidersChoked
			}
			return err
		}

		err = n.fetchPieceFromPeer(manifestCID, piece, source, status)
		if err == nil {
			return nil
		}
		if errors.Is(err, errPeerChoked) {
			logFetchDetail("piece %d: provider %s choked us; trying another provider if available", piece.Index, source)
			n.markPeerChokingUs(manifestCID, source)
			remaining = removeProvider(remaining, source)
			continue
		}
		return err
	}
	logFetchDetail("piece %d: ran out of non-choking providers in this round", piece.Index)
	return errAllProvidersChoked
}

// removeProvider returns the same provider list without one specific peer.
// We use this after a peer chokes us so we can try the same piece elsewhere
// during the current scheduling pass.
func removeProvider(providers []peer.ID, target peer.ID) []peer.ID {
	filtered := providers[:0]
	for _, provider := range providers {
		if provider == target {
			continue
		}
		filtered = append(filtered, provider)
	}
	return filtered
}

// recordPeerDownloadSample updates one peer's per-manifest download estimate.
// We turn one piece transfer into a bytes/sec sample, then smooth it so later
// peer choices are not dominated by a single noisy transfer.
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

	if n.ManifestPeerState == nil {
		n.ManifestPeerState = make(map[string]map[peer.ID]*PeerState)
	}
	peerStates, exists := n.ManifestPeerState[manifestCID]
	if !exists {
		peerStates = make(map[peer.ID]*PeerState)
		n.ManifestPeerState[manifestCID] = peerStates
	}

	peerState, exists := peerStates[peerID]
	if !exists {
		peerState = &PeerState{Choked: true}
		peerStates[peerID] = peerState
	}

	if peerState.SamplesDown == 0 {
		peerState.DownloadRate = sampleRate
	} else {
		alpha := currentSystemConfig().PeerDownloadRateAlpha
		peerState.DownloadRate = alpha*sampleRate + (1-alpha)*peerState.DownloadRate
	}
	peerState.SamplesDown++
}
