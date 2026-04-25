package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

// dummy DHT and dummy DHT operations used for testing
type fakeDHT struct{}

func (fakeDHT) Bootstrap(context.Context) error             { return nil }
func (fakeDHT) Provide(context.Context, string, bool) error { return nil }
func (fakeDHT) FindProviders(context.Context, string, int) ([]peer.AddrInfo, error) {
	return nil, nil
}
func (fakeDHT) Close() error { return nil }

type recordingDHT struct {
	provided []string
}

func (r *recordingDHT) Bootstrap(context.Context) error { return nil }
func (r *recordingDHT) Provide(_ context.Context, cid string, _ bool) error {
	r.provided = append(r.provided, cid)
	return nil
}
func (r *recordingDHT) FindProviders(context.Context, string, int) ([]peer.AddrInfo, error) {
	return nil, nil
}
func (r *recordingDHT) Close() error { return nil }

// Verifies that manifest construction splits a readable test file into the
// expected pieces and hashes both the manifest and each piece correctly.
func TestBuildManifestForReadablePieces(t *testing.T) {
	dir := t.TempDir() // Create a temporary folder for this test.
	path := filepath.Join(dir, "letters.txt")
	content := []byte("AAAA\nBBBB\nCCCC\n") // Create a test file containing exactly 15 bytes
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	// Build a manifest with piece size 5, each line should be a piece
	manifest, manifestBytes, manifestCID, err := BuildManifest(path, "letters.txt", 5)
	if err != nil {
		t.Fatal(err)
	}

	if manifestCID == "" || len(manifestBytes) == 0 {
		t.Fatal("expected manifest bytes and CID")
	}
	computedManifestCID, err := ComputeCIDFromBytes(manifestBytes)
	if err != nil {
		t.Fatal(err)
	}
	if computedManifestCID != manifestCID {
		t.Fatalf("manifest CID = %s, but bytes hash to %s", manifestCID, computedManifestCID)
	}
	if manifest.FileSize != int64(len(content)) {
		t.Fatalf("file size = %d, want %d", manifest.FileSize, len(content))
	}
	if len(manifest.Pieces) != 3 {
		t.Fatalf("piece count = %d, want 3", len(manifest.Pieces))
	}

	for i, want := range [][]byte{[]byte("AAAA\n"), []byte("BBBB\n"), []byte("CCCC\n")} {
		piece := manifest.Pieces[i]
		if piece.Index != i {
			t.Fatalf("piece index = %d, want %d", piece.Index, i)
		}
		if piece.Offset != int64(i*5) {
			t.Fatalf("piece offset = %d, want %d", piece.Offset, i*5)
		}
		if piece.Size != int64(len(want)) {
			t.Fatalf("piece size = %d, want %d", piece.Size, len(want))
		}
		wantCID, err := ComputeCIDFromBytes(want)
		if err != nil {
			t.Fatal(err)
		}
		if piece.CID != wantCID {
			t.Fatalf("piece CID = %s, want %s", piece.CID, wantCID)
		}
	}
}

// Verifies that a local rescan indexes one complete file into one manifest
// record plus the expected set of piece records.
func TestUpdateLocalObjectsIndexesManifestAndPieces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "letters.txt")
	if err := os.WriteFile(path, []byte("AAAA\nBBBB\nCCCC\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		ExportDir:     dir,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
		DHT:           fakeDHT{},
	}

	node.updateLocalObjects()

	var manifests, pieces int
	for _, record := range node.ServedObjects {
		switch record.Kind {
		case ObjectManifest:
			manifests++
			if record.PieceCount != 3 {
				t.Fatalf("manifest piece count = %d, want 3", record.PieceCount)
			}
			if _, err := os.Stat(record.Path); err != nil {
				t.Fatalf("manifest was not written: %v", err)
			}
		case ObjectPiece:
			pieces++
		}
	}

	if manifests != 1 || pieces != 3 {
		t.Fatalf("records: manifests=%d pieces=%d, want 1/3", manifests, pieces)
	}
}

// Verifies that local scanning advertises only the manifest CID to the DHT
// rather than every piece CID derived from that file.
func TestUpdateLocalObjectsAdvertisesOnlyManifestSwarms(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "letters.txt")
	if err := os.WriteFile(path, []byte("AAAA\nBBBB\nCCCC\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dht := &recordingDHT{}
	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		ExportDir:     dir,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
		DHT:           dht,
	}

	node.updateLocalObjects()

	var manifestCID string
	for cid, record := range node.ServedObjects {
		if record.Kind == ObjectManifest {
			manifestCID = cid
			break
		}
	}
	if manifestCID == "" {
		t.Fatal("expected manifest record")
	}

	if len(dht.provided) != 1 {
		t.Fatalf("provided CIDs = %v, want only the manifest CID", dht.provided)
	}
	if dht.provided[0] != manifestCID {
		t.Fatalf("provided CID = %s, want manifest %s", dht.provided[0], manifestCID)
	}
}

// Verifies that per-manifest availability reports only the pieces already
// marked present in DownloadState, preserving their manifest order.
func TestAvailabilityForManifestReportsPieceBitfield(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "letters.txt")
	content := []byte("AAAA\nBBBB\nCCCC\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	manifest, manifestBytes, manifestCID, err := BuildManifest(path, "letters.txt", 5)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		ExportDir:     dir,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
		DHT:           fakeDHT{},
	}

	node.startDownloadState(manifestCID, manifest, manifestStoragePath(dir, manifestCID), int64(len(manifestBytes)))
	node.markPieceAvailable(manifestCID, manifest.Pieces[0])
	node.markPieceAvailable(manifestCID, manifest.Pieces[2])

	availability, err := node.availabilityForManifest(manifestCID)
	if err != nil {
		t.Fatal(err)
	}

	want := []bool{true, false, true}
	if !reflect.DeepEqual(availability, want) {
		t.Fatalf("availability = %v, want %v", availability, want)
	}
}

// Verifies that a local rescan preserves piece availability for an in-progress
// download instead of dropping already cached pieces from ServedObjects.
func TestPartialDownloadSurvivesLocalObjectRescan(t *testing.T) {
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "letters.txt")
	content := []byte("AAAA\nBBBB\nCCCC\n")
	if err := os.WriteFile(sourcePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	manifest, manifestBytes, manifestCID, err := BuildManifest(sourcePath, "letters.txt", 5)
	if err != nil {
		t.Fatal(err)
	}

	destDir := t.TempDir()
	if err := ensureTinyTorrentDirs(destDir); err != nil {
		t.Fatal(err)
	}
	manifestPath := manifestStoragePath(destDir, manifestCID)
	if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
		t.Fatal(err)
	}

	firstPiece := manifest.Pieces[0]
	if err := os.WriteFile(pieceStoragePath(destDir, firstPiece.CID), content[:firstPiece.Size], 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		ExportDir:     destDir,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
		DHT:           fakeDHT{},
	}

	node.startDownloadState(manifestCID, manifest, manifestPath, int64(len(manifestBytes)))
	node.markPieceAvailable(manifestCID, firstPiece)
	node.updateLocalObjects()

	availability, err := node.availabilityForManifest(manifestCID)
	if err != nil {
		t.Fatal(err)
	}

	want := []bool{true, false, false}
	if !reflect.DeepEqual(availability, want) {
		t.Fatalf("availability after rescan = %v, want %v", availability, want)
	}
	if _, ok := node.ServedObjects[firstPiece.CID]; !ok {
		t.Fatalf("served objects lost partial piece %s after rescan", firstPiece.CID)
	}
}

// Verifies that once all piece bytes are cached locally, fetchFile can
// reconstruct and verify the final output file from those cached pieces.
func TestFinishPieceFetchReconstructsFromCachedPieces(t *testing.T) {
	// Create a source file
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "letters.txt")
	content := []byte("AAAA\nBBBB\nCCCC\n")
	if err := os.WriteFile(sourcePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	// Build a manifest
	manifest, manifestBytes, manifestCID, err := BuildManifest(sourcePath, "letters.txt", 5)
	if err != nil {
		t.Fatal(err)
	}

	// Create a destination directory where the “downloading peer” lives
	destDir := t.TempDir()
	if err := ensureTinyTorrentDirs(destDir); err != nil {
		t.Fatal(err)
	}

	// Manually write the piece files into the cache, pretend that the network fetch already downloaded each piece.
	for _, piece := range manifest.Pieces {
		start := piece.Offset
		end := start + piece.Size
		if err := os.WriteFile(pieceStoragePath(destDir, piece.CID), content[start:end], 0644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:           ctx,
		cancel:        cancel,
		ExportDir:     destDir,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
		DHT:           fakeDHT{},
	}

	// Creates the fake transfer response header that would normally come from a remote peer when downloading a manifest
	resp := TransferResponse{Kind: string(ObjectManifest), Filesize: int64(len(manifestBytes)), Filename: "letters.txt"}

	// Reconstruct the file
	if err := node.fetchFile(bytes.NewReader(manifestBytes), manifestCID, resp, nil); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(destDir, "letters.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("reconstructed content = %q, want %q", got, content)
	}
}
