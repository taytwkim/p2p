package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

type fakeDHT struct{}

func (fakeDHT) Bootstrap(context.Context) error             { return nil }
func (fakeDHT) Provide(context.Context, string, bool) error { return nil }
func (fakeDHT) FindProviders(context.Context, string, int) ([]peer.AddrInfo, error) {
	return nil, nil
}
func (fakeDHT) Close() error { return nil }

func TestBuildManifestForReadableChunks(t *testing.T) {
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
	if len(manifest.Chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(manifest.Chunks))
	}

	for i, want := range [][]byte{[]byte("AAAA\n"), []byte("BBBB\n"), []byte("CCCC\n")} {
		chunk := manifest.Chunks[i]
		if chunk.Index != i {
			t.Fatalf("chunk index = %d, want %d", chunk.Index, i)
		}
		if chunk.Offset != int64(i*5) {
			t.Fatalf("chunk offset = %d, want %d", chunk.Offset, i*5)
		}
		if chunk.Size != int64(len(want)) {
			t.Fatalf("chunk size = %d, want %d", chunk.Size, len(want))
		}
		wantCID, err := ComputeCIDFromBytes(want)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.CID != wantCID {
			t.Fatalf("chunk CID = %s, want %s", chunk.CID, wantCID)
		}
	}
}

func TestBuildManifestDemoChunksCID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "chunks.txt")
	content := []byte("AAAA\nBBBB\nCCCC\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	_, manifestBytes, manifestCID, err := BuildManifest(path, "chunks.txt", 5)
	if err != nil {
		t.Fatal(err)
	}

	computedManifestCID, err := ComputeCIDFromBytes(manifestBytes)
	if err != nil {
		t.Fatal(err)
	}
	if manifestCID != computedManifestCID {
		t.Fatalf("manifest CID = %s, but bytes hash to %s", manifestCID, computedManifestCID)
	}
}

func TestUpdateLocalFilesIndexesManifestFileAndChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "letters.txt")
	if err := os.WriteFile(path, []byte("AAAA\nBBBB\nCCCC\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:          ctx,
		cancel:       cancel,
		ExportDir:    dir,
		LocalFiles:   make(map[string]LocalFileRecord),
		ProvidedCIDs: make(map[string]struct{}),
		DHT:          fakeDHT{},
	}

	node.updateLocalFiles()

	var manifests, files, chunks int
	for _, record := range node.LocalFiles {
		switch record.Kind {
		case ObjectManifest:
			manifests++
			if record.ChunkCount != 3 {
				t.Fatalf("manifest chunk count = %d, want 3", record.ChunkCount)
			}
			if _, err := os.Stat(record.Path); err != nil {
				t.Fatalf("manifest was not written: %v", err)
			}
		case ObjectFile:
			files++
		case ObjectChunk:
			chunks++
		}
	}

	if manifests != 1 || files != 1 || chunks != 3 {
		t.Fatalf("records: manifests=%d files=%d chunks=%d, want 1/1/3", manifests, files, chunks)
	}
}

func TestFinishChunkedFetchReconstructsFromCachedChunks(t *testing.T) {
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
	if err := ensureP2PFSDirs(destDir); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range manifest.Chunks {
		start := chunk.Offset
		end := start + chunk.Size
		if err := os.WriteFile(chunkStoragePath(destDir, chunk.CID), content[start:end], 0644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := &Node{
		ctx:          ctx,
		cancel:       cancel,
		ExportDir:    destDir,
		LocalFiles:   make(map[string]LocalFileRecord),
		ProvidedCIDs: make(map[string]struct{}),
		DHT:          fakeDHT{},
	}

	resp := TransferResponse{Kind: string(ObjectManifest), Filesize: int64(len(manifestBytes)), Filename: "letters.txt"}
	if err := node.finishChunkedFetch(bytes.NewReader(manifestBytes), manifestCID, resp, "", nil); err != nil {
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
