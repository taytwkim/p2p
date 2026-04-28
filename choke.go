package main

import (
	"math/rand"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerState stores per-manifest observations and choke-related flags for one peer.
type PeerState struct {
	DownloadRate   float64
	UploadRate     float64
	SamplesDown    int
	SamplesUp      int
	Choked         bool
	Optimistic     bool
	RemoteChokesUs bool
	ChokedUntil    time.Time
	LastUnchoke    time.Time
}

// ensurePeerStateExists creates the per-manifest peer-state entry for one peer
// if it is missing. This lets selection treat the peer as known-but-unmeasured.
func (n *Node) ensurePeerStateExists(manifestCID string, peerID peer.ID) {
	if peerID == "" {
		return
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	if n.ManifestPeerState == nil {
		n.ManifestPeerState = make(map[string]map[peer.ID]*PeerState)
	}
	if _, exists := n.ManifestPeerState[manifestCID]; !exists {
		n.ManifestPeerState[manifestCID] = make(map[peer.ID]*PeerState)
	}
	if _, exists := n.ManifestPeerState[manifestCID][peerID]; !exists {
		n.ManifestPeerState[manifestCID][peerID] = &PeerState{Choked: true}
		n.reevaluateManifestChokingForLocked(manifestCID, time.Now())
	}
}

// isManifestCompleteLocked answers a simple question for choke policy:
// are we still downloading this manifest, or do we already have the complete file?
// The caller must already be holding stateLock.
func (n *Node) isManifestCompleteLocked(manifestCID string) bool {
	if _, downloading := n.DownloadState[manifestCID]; downloading {
		return false
	}
	_, complete := n.CompleteFiles[manifestCID]
	return complete
}

// runChokeReevaluationLoop periodically recomputes which known peers are
// unchoked for each manifest we currently track.
func (n *Node) runChokeReevaluationLoop() {
	ticker := time.NewTicker(currentSystemConfig().ChokeReevaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.reevaluateManifestChoking()
		}
	}
}

// reevaluateManifestChoking walks the known per-manifest peer pools and marks
// the top regular peers plus one optimistic peer as unchoked.
func (n *Node) reevaluateManifestChoking() {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	now := time.Now()
	for manifestCID, peers := range n.ManifestPeerState {
		if len(peers) == 0 {
			continue
		}
		n.reevaluateManifestChokingForLocked(manifestCID, now)
	}
}

// reevaluateManifestChokingForLocked recalculates one manifest's upload slots in place.
// It first assigns the regular unchoke slots by measured rate, then picks one
// remaining peer at random for the optimistic slot. The caller holds stateLock.
func (n *Node) reevaluateManifestChokingForLocked(manifestCID string, now time.Time) {
	peers := n.ManifestPeerState[manifestCID]
	if len(peers) == 0 {
		return
	}

	complete := n.isManifestCompleteLocked(manifestCID)
	orderedPeers := rankPeersForUnchoking(peers, complete)
	for _, peerState := range peers {
		peerState.Choked = true
		peerState.Optimistic = false
	}

	// Keep a simple reevaluation counter per manifest so we can hold the same
	// optimistic peer steady for three rounds before rotating it.
	n.RechokeRounds[manifestCID]++
	rechokeRound := n.RechokeRounds[manifestCID]

	optimisticSlots := currentSystemConfig().OptimisticUnchokeSlots
	regularSlots := currentSystemConfig().MaxUnchokedPeersPerManifest - optimisticSlots
	if regularSlots < 0 {
		regularSlots = 0
	}

	unchoked := make(map[peer.ID]struct{})
	for i := 0; i < len(orderedPeers) && i < regularSlots; i++ {
		peerID := orderedPeers[i]
		peerState := peers[peerID]
		peerState.Choked = false
		peerState.LastUnchoke = now
		unchoked[peerID] = struct{}{}
	}

	if optimisticSlots > 0 {
		remaining := make([]peer.ID, 0, len(orderedPeers))
		for _, peerID := range orderedPeers {
			if _, alreadyChosen := unchoked[peerID]; alreadyChosen {
				continue
			}
			remaining = append(remaining, peerID)
		}
		if len(remaining) > 0 {
			peerID := n.chooseOptimisticPeer(manifestCID, remaining, rechokeRound)
			peerState := peers[peerID]
			peerState.Choked = false
			peerState.Optimistic = true
			peerState.LastUnchoke = now
			n.OptimisticPeers[manifestCID] = peerID
		} else {
			delete(n.OptimisticPeers, manifestCID)
		}
	}
}

// chooseOptimisticPeer keeps the current optimistic peer until the third
// reevaluation round, unless that peer is no longer eligible for the extra slot.
func (n *Node) chooseOptimisticPeer(manifestCID string, remaining []peer.ID, rechokeRound int) peer.ID {
	current := n.OptimisticPeers[manifestCID]
	rotationRounds := currentSystemConfig().OptimisticUnchokeRotationRounds
	shouldRotate := rechokeRound == 1 || rechokeRound%rotationRounds == 1

	if !shouldRotate {
		for _, peerID := range remaining {
			if peerID == current {
				return peerID
			}
		}
	}

	// On a rotation round, try to pick someone new when we have a choice.
	if len(remaining) > 1 {
		candidates := make([]peer.ID, 0, len(remaining))
		for _, peerID := range remaining {
			if peerID == current {
				continue
			}
			candidates = append(candidates, peerID)
		}
		if len(candidates) > 0 {
			return candidates[rand.Intn(len(candidates))]
		}
	}

	return remaining[rand.Intn(len(remaining))]
}

// rankPeersForUnchoking orders known peers for one manifest by the metric that
// matters in the current role: download rate while the download is incomplete,
// upload rate once complete.
func rankPeersForUnchoking(peers map[peer.ID]*PeerState, complete bool) []peer.ID {
	ordered := make([]peer.ID, 0, len(peers))
	for peerID := range peers {
		ordered = append(ordered, peerID)
	}

	scoreFor := func(state *PeerState) float64 {
		if complete {
			return state.UploadRate
		}
		return state.DownloadRate
	}

	sort.SliceStable(ordered, func(i, j int) bool {
		left := peers[ordered[i]]
		right := peers[ordered[j]]
		leftScore := scoreFor(left)
		rightScore := scoreFor(right)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return ordered[i].String() < ordered[j].String()
	})
	return ordered
}

// isPeerUnchokedForManifest checks whether this peer is currently allowed to
// download pieces for this manifest from us. The serve path uses this to turn
// away requests from peers outside the current unchoke set.
func (n *Node) isPeerUnchokedForManifest(manifestCID string, peerID peer.ID) bool {
	n.stateLock.RLock()
	defer n.stateLock.RUnlock()

	peerStates := n.ManifestPeerState[manifestCID]
	if peerStates == nil {
		return false
	}
	peerState := peerStates[peerID]
	if peerState == nil {
		return false
	}
	return !peerState.Choked
}

// recordPeerUploadSample updates our measured upload rate to one peer for one
// manifest. This is the signal the choke loop uses once we have completed downloading
// the file and is now the seeder.
func (n *Node) recordPeerUploadSample(manifestCID string, peerID peer.ID, bytes int64, elapsed time.Duration) {
	if peerID == "" || bytes <= 0 {
		return
	}

	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	sampleRate := float64(bytes) / seconds

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	if n.ManifestPeerState == nil {
		n.ManifestPeerState = make(map[string]map[peer.ID]*PeerState)
	}
	if _, exists := n.ManifestPeerState[manifestCID]; !exists {
		n.ManifestPeerState[manifestCID] = make(map[peer.ID]*PeerState)
	}
	peerState, exists := n.ManifestPeerState[manifestCID][peerID]
	if !exists {
		peerState = &PeerState{Choked: true}
		n.ManifestPeerState[manifestCID][peerID] = peerState
	}

	if peerState.SamplesUp == 0 {
		peerState.UploadRate = sampleRate
	} else {
		alpha := currentSystemConfig().PeerDownloadRateAlpha
		peerState.UploadRate = alpha*sampleRate + (1-alpha)*peerState.UploadRate
	}
	peerState.SamplesUp++
}

// markPeerChokingUs records that a remote peer refused our piece request for
// this manifest. Until ChokedUntil expires, peer selection skips that peer
// instead of retrying it immediately in a tight loop.
func (n *Node) markPeerChokingUs(manifestCID string, peerID peer.ID) {
	if peerID == "" {
		return
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	if n.ManifestPeerState == nil {
		n.ManifestPeerState = make(map[string]map[peer.ID]*PeerState)
	}
	if _, exists := n.ManifestPeerState[manifestCID]; !exists {
		n.ManifestPeerState[manifestCID] = make(map[peer.ID]*PeerState)
	}
	peerState, exists := n.ManifestPeerState[manifestCID][peerID]
	if !exists {
		peerState = &PeerState{Choked: true}
		n.ManifestPeerState[manifestCID][peerID] = peerState
	}
	peerState.RemoteChokesUs = true
	peerState.ChokedUntil = time.Now().Add(currentSystemConfig().ChokeReevaluationInterval)
}

// nextChokeRetryDelay chooses how long the downloader should wait before it
// retries pieces whose providers are all currently choking us. It looks for the
// soonest provider cooldown to expire and uses that as the next retry point.
// Those cooldowns are currently measured using chokeReevaluationInterval too.
func (n *Node) nextChokeRetryDelay(manifestCID string, pieces []ManifestPiece, pieceSources map[string][]peer.ID) time.Duration {
	n.stateLock.RLock()
	defer n.stateLock.RUnlock()

	now := time.Now()
	delay := currentSystemConfig().ChokeReevaluationInterval
	peerStates := n.ManifestPeerState[manifestCID]
	for _, piece := range pieces {
		for _, provider := range pieceSources[piece.CID] {
			peerState := peerStates[provider]
			if peerState == nil || peerState.ChokedUntil.IsZero() {
				continue
			}
			if !now.Before(peerState.ChokedUntil) {
				return time.Second
			}
			candidate := time.Until(peerState.ChokedUntil)
			if candidate < delay {
				delay = candidate
			}
		}
	}
	if delay < time.Second {
		return time.Second
	}
	return delay
}
