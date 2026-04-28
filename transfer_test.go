package main

import (
	"bytes"
	"context"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Verifies that the transfer framing helper returns a body reader positioned
// exactly at the payload bytes, without leaking the header delimiter.
func TestTransferResponseHeaderDoesNotLeakDelimiterIntoBody(t *testing.T) {
	var stream bytes.Buffer
	body := []byte("{manifest-json-starts-like-this}")

	if err := writeTransferResponseHeader(&stream, TransferResponse{
		Kind:     string(ObjectManifest),
		Filesize: int64(len(body)),
		Filename: "manifest.json",
	}); err != nil {
		t.Fatal(err)
	}
	stream.Write(body)

	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(&stream, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Kind != string(ObjectManifest) {
		t.Fatalf("kind = %q, want %q", resp.Kind, ObjectManifest)
	}

	got, err := io.ReadAll(io.LimitReader(bodyReader, resp.Filesize))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// Verifies that self-provider filtering removes this node from the provider list.
func TestFilterSelfProviderCandidates(t *testing.T) {
	self := peer.ID("self")
	other := peer.ID("other")
	providers := []peer.AddrInfo{
		{ID: self},
		{ID: other},
		{ID: self},
	}

	got := filterSelfProviderCandidates(providers, self)
	if len(got) != 1 {
		t.Fatalf("filtered providers = %v, want only other provider", got)
	}
	if got[0].ID != other {
		t.Fatalf("filtered provider = %s, want %s", got[0].ID, other)
	}
}

// Verifies that the selector ranks missing pieces by rarity
func TestRankPiecesRarestFirst(t *testing.T) {
	pieces := []ManifestPiece{
		{Index: 0, CID: "piece-0"},
		{Index: 1, CID: "piece-1"},
		{Index: 2, CID: "piece-2"},
	}
	pieceSources := map[string][]peer.ID{
		"piece-0": []peer.ID{peer.ID("peer-a"), peer.ID("peer-b")},
		"piece-1": []peer.ID{peer.ID("peer-a")},
		"piece-2": []peer.ID{peer.ID("peer-a"), peer.ID("peer-b"), peer.ID("peer-c")},
	}

	ranked := rankPiecesRarestFirst(pieces, pieceSources)
	if ranked[0].CID != "piece-1" || ranked[1].CID != "piece-0" || ranked[2].CID != "piece-2" {
		t.Fatalf("ranked pieces = %#v", ranked)
	}
}

// Verifies that a brand-new download starts with one random bootstrap piece
// instead of immediately scheduling the full rarest-first list.
func TestSchedulePiecesForRoundBootstrapsWithOneRandomPiece(t *testing.T) {
	pieces := []ManifestPiece{
		{Index: 0, CID: "piece-0"},
		{Index: 1, CID: "piece-1"},
		{Index: 2, CID: "piece-2"},
	}
	pieceSources := map[string][]peer.ID{
		"piece-0": {peer.ID("peer-a"), peer.ID("peer-b")},
		"piece-1": {peer.ID("peer-a")},
		"piece-2": {peer.ID("peer-a"), peer.ID("peer-b"), peer.ID("peer-c")},
	}

	scheduled := schedulePiecesForRound(pieces, pieceSources, false)
	if len(scheduled) != 1 {
		t.Fatalf("scheduled %d bootstrap pieces, want 1", len(scheduled))
	}

	chosen := scheduled[0].CID
	if chosen != "piece-0" && chosen != "piece-1" && chosen != "piece-2" {
		t.Fatalf("bootstrap piece = %q, want one of the missing pieces", chosen)
	}
}

// Verifies that once we already have a verified piece, scheduling returns to
// the normal rarest-first ordering.
func TestSchedulePiecesForRoundUsesRarestFirstAfterBootstrap(t *testing.T) {
	pieces := []ManifestPiece{
		{Index: 0, CID: "piece-0"},
		{Index: 1, CID: "piece-1"},
		{Index: 2, CID: "piece-2"},
	}
	pieceSources := map[string][]peer.ID{
		"piece-0": {peer.ID("peer-a"), peer.ID("peer-b")},
		"piece-1": {peer.ID("peer-a")},
		"piece-2": {peer.ID("peer-a"), peer.ID("peer-b"), peer.ID("peer-c")},
	}

	scheduled := schedulePiecesForRound(pieces, pieceSources, true)
	if scheduled[0].CID != "piece-1" || scheduled[1].CID != "piece-0" || scheduled[2].CID != "piece-2" {
		t.Fatalf("scheduled pieces = %#v", scheduled)
	}
}

// Verifies that peer choice explores unknown peers before using measured
// download rates, then falls back to the fastest known peer.
func TestChoosePeerForPiecePrefersUnknownThenFastestKnown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manifestCID := "manifest-1"
	piece := ManifestPiece{Index: 0, CID: "piece-0"}
	providers := []peer.ID{peer.ID("peer-a"), peer.ID("peer-b"), peer.ID("peer-c")}

	node := &Node{
		ctx:               ctx,
		cancel:            cancel,
		ManifestPeerState: make(map[string]map[peer.ID]*PeerState),
	}
	node.ManifestPeerState[manifestCID] = map[peer.ID]*PeerState{
		peer.ID("peer-a"): {DownloadRate: 100, SamplesDown: 2},
		peer.ID("peer-b"): {DownloadRate: 400, SamplesDown: 3},
	}

	chosen, err := node.choosePeerForPiece(manifestCID, piece, providers)
	if err != nil {
		t.Fatal(err)
	}
	if chosen != peer.ID("peer-c") {
		t.Fatalf("chosen unknown peer = %s, want peer-c", chosen)
	}

	node.ManifestPeerState[manifestCID][peer.ID("peer-c")] = &PeerState{DownloadRate: 250, SamplesDown: 1}
	chosen, err = node.choosePeerForPiece(manifestCID, piece, providers)
	if err != nil {
		t.Fatal(err)
	}
	if chosen != peer.ID("peer-b") {
		t.Fatalf("chosen fastest known peer = %s, want peer-b", chosen)
	}
}

// Verifies that per-manifest peer download rates are updated from transfer
// samples using the configured smoothing rule.
func TestRecordPeerDownloadSampleUsesMovingAverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manifestCID := "manifest-1"
	peerID := peer.ID("peer-a")
	node := &Node{
		ctx:               ctx,
		cancel:            cancel,
		ManifestPeerState: make(map[string]map[peer.ID]*PeerState),
	}

	node.recordPeerDownloadSample(manifestCID, peerID, 100, time.Second)
	first := node.ManifestPeerState[manifestCID][peerID]
	if first.SamplesDown != 1 || first.DownloadRate != 100 {
		t.Fatalf("first sample = %+v, want rate=100 samples=1", first)
	}

	node.recordPeerDownloadSample(manifestCID, peerID, 200, time.Second)
	second := node.ManifestPeerState[manifestCID][peerID]
	alpha := currentSystemConfig().PeerDownloadRateAlpha
	want := alpha*200 + (1-alpha)*100
	if second.SamplesDown != 2 || math.Abs(second.DownloadRate-want) > 0.0001 {
		t.Fatalf("second sample = %+v, want rate=%f samples=2", second, want)
	}
}

// Verifies that choke reevaluation keeps one optimistic slot and uses
// download-rate ranking while the manifest is still incomplete.
func TestReevaluateManifestChokingUsesOptimisticSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manifestCID := "manifest-1"
	now := time.Now()
	node := &Node{
		ctx:               ctx,
		cancel:            cancel,
		DownloadState:     map[string]*FileDownloadState{manifestCID: {ManifestCID: manifestCID}},
		ManifestPeerState: make(map[string]map[peer.ID]*PeerState),
		OptimisticPeers:   make(map[string]peer.ID),
		RechokeRounds:     make(map[string]int),
	}
	node.ManifestPeerState[manifestCID] = map[peer.ID]*PeerState{
		peer.ID("peer-a"): {DownloadRate: 100},
		peer.ID("peer-b"): {DownloadRate: 80},
		peer.ID("peer-c"): {DownloadRate: 60},
		peer.ID("peer-d"): {DownloadRate: 40},
		peer.ID("peer-e"): {DownloadRate: 20},
	}

	node.stateLock.Lock()
	node.reevaluateManifestChokingForLocked(manifestCID, now)
	node.stateLock.Unlock()

	unchoked := 0
	optimistic := 0
	for _, state := range node.ManifestPeerState[manifestCID] {
		if !state.Choked {
			unchoked++
		}
		if state.Optimistic {
			optimistic++
		}
	}
	maxUnchoked := currentSystemConfig().MaxUnchokedPeersPerManifest
	if unchoked != maxUnchoked {
		t.Fatalf("unchoked peers = %d, want %d", unchoked, maxUnchoked)
	}
	if optimistic != 1 {
		t.Fatalf("optimistic slots = %d, want 1", optimistic)
	}
	optimisticPeer := peer.ID("")
	for peerID, state := range node.ManifestPeerState[manifestCID] {
		if state.Optimistic {
			optimisticPeer = peerID
			break
		}
	}
	if optimisticPeer != peer.ID("peer-d") && optimisticPeer != peer.ID("peer-e") {
		t.Fatalf("optimistic peer = %s, want one of the remaining non-regular peers", optimisticPeer)
	}
}

// Verifies that we still reevaluate every round, but we only rotate the
// optimistic peer every third reevaluation.
func TestOptimisticUnchokeRotatesEveryThirdReevaluation(t *testing.T) {
	rand.Seed(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manifestCID := "manifest-1"
	now := time.Now()
	node := &Node{
		ctx:               ctx,
		cancel:            cancel,
		DownloadState:     map[string]*FileDownloadState{manifestCID: {ManifestCID: manifestCID}},
		ManifestPeerState: make(map[string]map[peer.ID]*PeerState),
		OptimisticPeers:   make(map[string]peer.ID),
		RechokeRounds:     make(map[string]int),
	}
	node.ManifestPeerState[manifestCID] = map[peer.ID]*PeerState{
		peer.ID("peer-a"): {DownloadRate: 100},
		peer.ID("peer-b"): {DownloadRate: 90},
		peer.ID("peer-c"): {DownloadRate: 80},
		peer.ID("peer-d"): {DownloadRate: 70},
		peer.ID("peer-e"): {DownloadRate: 60},
		peer.ID("peer-f"): {DownloadRate: 50},
	}

	node.stateLock.Lock()
	node.reevaluateManifestChokingForLocked(manifestCID, now)
	first := node.OptimisticPeers[manifestCID]
	node.reevaluateManifestChokingForLocked(manifestCID, now.Add(10*time.Second))
	second := node.OptimisticPeers[manifestCID]
	node.reevaluateManifestChokingForLocked(manifestCID, now.Add(20*time.Second))
	third := node.OptimisticPeers[manifestCID]
	node.reevaluateManifestChokingForLocked(manifestCID, now.Add(30*time.Second))
	fourth := node.OptimisticPeers[manifestCID]
	rounds := node.RechokeRounds[manifestCID]
	node.stateLock.Unlock()

	if first == "" {
		t.Fatal("first optimistic peer was not chosen")
	}
	if second != first {
		t.Fatalf("second optimistic peer = %s, want same as first %s", second, first)
	}
	if third != first {
		t.Fatalf("third optimistic peer = %s, want same as first %s", third, first)
	}
	if fourth == "" {
		t.Fatal("fourth optimistic peer was not chosen")
	}
	if fourth == first {
		t.Fatalf("fourth optimistic peer = %s, want a different peer after rotation", fourth)
	}
	if rounds != 4 {
		t.Fatalf("rechoke rounds = %d, want 4", rounds)
	}
}
