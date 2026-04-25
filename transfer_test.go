package main

import (
	"bytes"
	"context"
	"io"
	"math"
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

// Verifies that peer choice explores unknown peers before using measured
// download rates, then falls back to the fastest known peer.
func TestChoosePeerForPiecePrefersUnknownThenFastestKnown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manifestCID := "manifest-1"
	piece := ManifestPiece{Index: 0, CID: "piece-0"}
	providers := []peer.ID{peer.ID("peer-a"), peer.ID("peer-b"), peer.ID("peer-c")}

	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		DownloadState: make(map[string]*FileDownloadState),
	}
	node.DownloadState[manifestCID] = &FileDownloadState{
		ManifestCID: manifestCID,
		PeerStats: map[peer.ID]*PeerState{
			peer.ID("peer-a"): {DownloadRate: 100, SamplesDown: 2},
			peer.ID("peer-b"): {DownloadRate: 400, SamplesDown: 3},
		},
	}

	chosen, err := node.choosePeerForPiece(manifestCID, piece, providers)
	if err != nil {
		t.Fatal(err)
	}
	if chosen != peer.ID("peer-c") {
		t.Fatalf("chosen unknown peer = %s, want peer-c", chosen)
	}

	node.DownloadState[manifestCID].PeerStats[peer.ID("peer-c")] = &PeerState{DownloadRate: 250, SamplesDown: 1}
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
		ctx:           ctx,
		cancel:        cancel,
		DownloadState: make(map[string]*FileDownloadState),
	}
	node.DownloadState[manifestCID] = &FileDownloadState{
		ManifestCID: manifestCID,
		PeerStats:   make(map[peer.ID]*PeerState),
	}

	node.recordPeerDownloadSample(manifestCID, peerID, 100, time.Second)
	first := node.DownloadState[manifestCID].PeerStats[peerID]
	if first.SamplesDown != 1 || first.DownloadRate != 100 {
		t.Fatalf("first sample = %+v, want rate=100 samples=1", first)
	}

	node.recordPeerDownloadSample(manifestCID, peerID, 200, time.Second)
	second := node.DownloadState[manifestCID].PeerStats[peerID]
	want := peerDownloadRateAlpha*200 + (1-peerDownloadRateAlpha)*100
	if second.SamplesDown != 2 || math.Abs(second.DownloadRate-want) > 0.0001 {
		t.Fatalf("second sample = %+v, want rate=%f samples=2", second, want)
	}
}
