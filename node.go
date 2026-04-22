package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

/*
 * We organize local contents (files, manifests, and pieces) in the following way.
 *
 * 		1. CompleteFiles
 *    		- Tracks files that have been fully downloaded and available in the export directory.
 *    		- Keyed by manifest CID.
 *    		- Each entry stores the parsed manifest, the manifest file path, and all
 *      		CID-addressed objects for that file (the manifest itself plus every piece).
 *
 * 		2. DownloadState
 *    		- Tracks files that are still being downloaded.
 *    		- Keyed by manifest CID.
 *    		- Stores the manifest, where the manifest is cached on disk, and a
 *				Have[] bitmap saying which pieces have already been downloaded.
 *    		- This is the node's in-progress download bookkeeping.
 *
 * 		3. ServedObjects
 *    		- Tracks everything this node can serve right now, keyed by CID.
 *    		- Includes:
 *        		a) manifest + piece objects for fully complete files in CompleteFiles
 *        		b) manifest + already-downloaded piece objects for in-progress files in DownloadState
 *
 * FILESYSTEM LAYOUT
 *
 * 		exportDir/
 *   		<complete files>
 *   	.p2pfs/
 *     		manifests/
 *       		<manifestCID>    cached manifest JSON files
 *     		pieces/
 *       		<pieceCID>       cached piece bytes
 *
 * HIGH-LEVEL FLOW
 *
 * 		- Complete local files in ExportDir are scanned into CompleteFiles.
 * 		- Partial downloads are tracked in DownloadState as pieces arrive.
 * 		- ServedObjects is rebuilt from those two sources so the node always knows
 *   		which CIDs it can serve immediately.
 * 		- The DHT advertises only manifest CIDs. A manifest CID identifies the swarm
 *   		for a file; piece ownership is exchanged directly between peers.
 */

// LocalObjectRecord describes one CID-addressed object this node can serve.
type LocalObjectRecord struct {
	CID         string          // The CID for this object.
	Kind        LocalObjectKind // Whether this record is a manifest or piece.
	Filename    string          // Human-friendly name.
	Path        string
	Size        int64
	Offset      int64     // For pieces served from a larger file, where the piece starts.
	Length      int64     // For pieces, how many bytes to serve.
	ManifestCID string    // The manifest CID for the file this object belongs to.
	PieceCount  int       // Number of pieces in the file.
	Manifest    *Manifest // Parsed manifest object, useful for manifest records.
}

type LocalObjectKind string

const (
	ObjectManifest LocalObjectKind = "manifest"
	ObjectPiece    LocalObjectKind = "piece"
)

type CompleteFile struct {
	ManifestCID  string
	Manifest     *Manifest
	ManifestPath string
	ManifestSize int64
	Objects      map[string]LocalObjectRecord
}

type FileDownloadState struct {
	ManifestCID  string
	Manifest     *Manifest
	ManifestPath string
	ManifestSize int64
	Have         []bool
}

// Node is a p2p daemon
type Node struct {
	ctx           context.Context // ctx and cancel are used to manage the lifecycle of daemons.
	cancel        context.CancelFunc
	Host          host.Host // core engine provided by libp2p, representing your presence on the network.
	ExportDir     string    // local path to the folder where shared files live.
	RpcSocket     string    // path to the local Unix Domain Socket used for CLI commands.
	CompleteFiles map[string]CompleteFile
	DownloadState map[string]*FileDownloadState
	ServedObjects map[string]LocalObjectRecord // all local objects this node can serve, keyed by CID.
	stateLock     sync.RWMutex                 // protects CompleteFiles, DownloadState, and ServedObjects.
	DHT           DHTNode                      // Kademlia DHT used for provider registration and lookup.
	ProvidedCIDs  map[string]struct{}          // manifest CIDs already announced into the DHT as file swarms.
	providedLock  sync.Mutex
	rpcListener   net.Listener // rpcListener holds the open Unix Domain Socket listener for CLI clients.
}

// NewNode initializes a new libp2p node, connects to bootstrap nodes, and starts background tasks
func NewNode(listenAddr, exportDir, rpcSocket string, bootstrapAddrs []string) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. Create libp2p Host
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	n := &Node{
		ctx:           ctx,
		cancel:        cancel,
		Host:          h,
		ExportDir:     exportDir,
		RpcSocket:     rpcSocket,
		CompleteFiles: make(map[string]CompleteFile),
		DownloadState: make(map[string]*FileDownloadState),
		ServedObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs:  make(map[string]struct{}),
	}

	log.Printf("Host created. Our Peer ID: %s", h.ID().String())
	for _, addr := range h.Addrs() {
		log.Printf("Listening on: %s/p2p/%s", addr, h.ID())
	}

	// 2. Setup DHT
	dhtNode, err := NewDHTNode(ctx, h, bootstrapAddrs)
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}
	n.DHT = dhtNode

	// 3. Connect to bootstrap peers
	n.connectBootstrappers(bootstrapAddrs)

	if err := n.DHT.Bootstrap(ctx); err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	// 4. Register RPC and background tasks
	// "RPC server" is the endpoint that nodes expose
	// to accept CLI-issued commands
	if n.RpcSocket != "" {
		if err := n.startRPCServer(); err != nil {
			h.Close()
			cancel()
			return nil, err
		}
	}

	// 5. Start scanning local directory periodically
	go n.scanLocalObjects()

	// 6. Register protocols
	n.setupTransferProtocol()
	n.setupIndexProtocol()

	return n, nil
}

func (n *Node) Close() error {
	n.cancel()
	if n.rpcListener != nil {
		n.rpcListener.Close()
	}
	if n.DHT != nil {
		n.DHT.Close()
	}
	return n.Host.Close()
}

// parses multiaddrs of bootstrap nodes and connects to them
func (n *Node) connectBootstrappers(addrs []string) {
	var wg sync.WaitGroup
	// iterate list of known bootstrap nodes and try to connect to ALL of them
	for _, addrStr := range addrs {
		addrStr := addrStr // capture loop vars
		if addrStr == "" {
			continue
		}

		// take IP and convert to protocol-agnostic multiaddr
		maddr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			log.Printf("Invalid bootstrap address %s: %v", addrStr, err)
			continue
		}

		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			log.Printf("Invalid bootstrap info %s: %v", addrStr, err)
			continue
		}

		wg.Add(1)

		// This part (the go routine) is non-blocking, so that one failed attempt
		// does not stall. So we will attempt to connect to all bootstrap nodes.
		go func(info peer.AddrInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := n.Host.Connect(ctx, info); err != nil {
				log.Printf("Could not connect to bootstrap peer %s: %v", info.ID, err)
			} else {
				log.Printf("Connected to bootstrap peer %s", info.ID)
			}
		}(*info)
	}
	wg.Wait()
}

// Wrapper that calls updateLocalObjects periodically
func (n *Node) scanLocalObjects() {
	// We poll because we want to check whether the user has uploaded a new file in export_dir
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run once immediately
	n.updateLocalObjects()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.updateLocalObjects()
		}
	}
}

func (n *Node) updateLocalObjects() {
	if err := ensureP2PFSDirs(n.ExportDir); err != nil {
		log.Printf("Error creating internal storage dirs: %v", err)
		return
	}

	files, err := os.ReadDir(n.ExportDir)
	if err != nil {
		log.Printf("Error reading export dir: %v", err)
		return
	}

	completeFiles := make(map[string]CompleteFile)
	for _, f := range files {
		if f.IsDir() || f.Name() == ".p2pfs" || strings.HasSuffix(f.Name(), ".downloading") {
			continue
		}
		path := filepath.Join(n.ExportDir, f.Name())

		manifest, manifestBytes, manifestCID, err := BuildManifest(path, f.Name(), defaultPieceSize)
		if err != nil {
			log.Printf("Error building manifest for %s: %v", f.Name(), err)
			continue
		}
		manifestPath := manifestStoragePath(n.ExportDir, manifestCID)
		if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
			log.Printf("Error writing manifest for %s: %v", f.Name(), err)
			continue
		}

		objects := make(map[string]LocalObjectRecord)
		objects[manifestCID] = LocalObjectRecord{
			CID:         manifestCID,
			Kind:        ObjectManifest,
			Filename:    f.Name(),
			Path:        manifestPath,
			Size:        int64(len(manifestBytes)),
			ManifestCID: manifestCID,
			PieceCount:  len(manifest.Pieces),
			Manifest:    manifest,
		}

		for _, piece := range manifest.Pieces {
			objects[piece.CID] = LocalObjectRecord{
				CID:         piece.CID,
				Kind:        ObjectPiece,
				Filename:    fmt.Sprintf("%s.piece-%d", f.Name(), piece.Index),
				Path:        path,
				Size:        piece.Size,
				Offset:      piece.Offset,
				Length:      piece.Size,
				ManifestCID: manifestCID,
			}
		}

		completeFiles[manifestCID] = CompleteFile{
			ManifestCID:  manifestCID,
			Manifest:     manifest,
			ManifestPath: manifestPath,
			ManifestSize: int64(len(manifestBytes)),
			Objects:      objects,
		}
	}

	n.stateLock.Lock()
	n.CompleteFiles = completeFiles
	n.rebuildServedObjectsLocked()
	servedObjects := cloneServedObjects(n.ServedObjects)
	n.stateLock.Unlock()

	n.provideNewObjectCIDs(servedObjects)
}

// rebuilds ServedObjects so the manifest and any downloaded pieces
// are exposed as servable objects.
func (n *Node) rebuildServedObjectsLocked() {
	served := make(map[string]LocalObjectRecord)
	for _, file := range n.CompleteFiles {
		for cid, record := range file.Objects {
			served[cid] = record
		}
	}

	for manifestCID, state := range n.DownloadState {
		if state == nil || state.Manifest == nil {
			continue
		}
		served[manifestCID] = LocalObjectRecord{
			CID:         manifestCID,
			Kind:        ObjectManifest,
			Filename:    state.Manifest.Filename,
			Path:        state.ManifestPath,
			Size:        state.ManifestSize,
			ManifestCID: manifestCID,
			PieceCount:  len(state.Manifest.Pieces),
			Manifest:    state.Manifest,
		}
		for i, have := range state.Have {
			if !have || i >= len(state.Manifest.Pieces) {
				continue
			}
			piece := state.Manifest.Pieces[i]
			served[piece.CID] = LocalObjectRecord{
				CID:         piece.CID,
				Kind:        ObjectPiece,
				Filename:    fmt.Sprintf("%s.piece-%d", state.Manifest.Filename, piece.Index),
				Path:        pieceStoragePath(n.ExportDir, piece.CID),
				Size:        piece.Size,
				Length:      piece.Size,
				ManifestCID: manifestCID,
			}
		}
	}

	n.ServedObjects = served
}

// Instead of holding onto the lock, release the lock
// when we have a stable view of the servable objects
func cloneServedObjects(objects map[string]LocalObjectRecord) map[string]LocalObjectRecord {
	clone := make(map[string]LocalObjectRecord, len(objects))
	for cid, record := range objects {
		clone[cid] = record
	}
	return clone
}

// Creates or initializes DownloadState entry for a manifest
// “we are now tracking this file as an in-progress download”
func (n *Node) startDownloadState(manifestCID string, manifest *Manifest, manifestPath string, manifestSize int64) {
	n.stateLock.Lock()
	if _, alreadyComplete := n.CompleteFiles[manifestCID]; alreadyComplete {
		n.stateLock.Unlock()
		return
	}

	state, exists := n.DownloadState[manifestCID]
	if !exists {
		state = &FileDownloadState{
			ManifestCID:  manifestCID,
			Manifest:     manifest,
			ManifestPath: manifestPath,
			ManifestSize: manifestSize,
			Have:         make([]bool, len(manifest.Pieces)),
		}
		n.DownloadState[manifestCID] = state
	} else {
		state.Manifest = manifest
		state.ManifestPath = manifestPath
		state.ManifestSize = manifestSize
		if len(state.Have) != len(manifest.Pieces) {
			state.Have = make([]bool, len(manifest.Pieces))
		}
	}
	n.rebuildServedObjectsLocked()
	servedObjects := cloneServedObjects(n.ServedObjects)
	n.stateLock.Unlock()

	n.provideNewObjectCIDs(servedObjects)
}

// updates DownloadState entry by marking a piece as downloaded
func (n *Node) markPieceAvailable(manifestCID string, piece ManifestPiece) {
	n.stateLock.Lock()
	state := n.DownloadState[manifestCID]
	if state != nil && piece.Index >= 0 && piece.Index < len(state.Have) {
		state.Have[piece.Index] = true
		n.rebuildServedObjectsLocked()
	}
	n.stateLock.Unlock()
}

// deletes DownloadState[manifestCID] when we have finished downloading
func (n *Node) clearDownloadState(manifestCID string) {
	n.stateLock.Lock()
	delete(n.DownloadState, manifestCID)
	n.rebuildServedObjectsLocked()
	n.stateLock.Unlock()
}

// helper that creates .p2pfs/manifests and .p2pfs/pieces
func ensureP2PFSDirs(exportDir string) error {
	if err := os.MkdirAll(filepath.Join(exportDir, ".p2pfs", "manifests"), 0755); err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(exportDir, ".p2pfs", "pieces"), 0755)
}

// helper that returns the filepath where a manifest JSON file should be stored.
func manifestStoragePath(exportDir, manifestCID string) string {
	return filepath.Join(exportDir, ".p2pfs", "manifests", manifestCID+".json")
}

// returns the filepath where a downloaded piece should be cached.
func pieceStoragePath(exportDir, pieceCID string) string {
	return filepath.Join(exportDir, ".p2pfs", "pieces", pieceCID)
}

// announces manifest CIDs to the DHT
func (n *Node) provideNewObjectCIDs(objects map[string]LocalObjectRecord) {
	n.providedLock.Lock()
	defer n.providedLock.Unlock()

	current := make(map[string]struct{}, len(objects))
	for cidStr, record := range objects {
		if record.Kind != ObjectManifest {
			continue
		}

		current[cidStr] = struct{}{}
		if _, alreadyProvided := n.ProvidedCIDs[cidStr]; alreadyProvided {
			continue
		}

		if err := n.DHT.Provide(n.ctx, cidStr, true); err != nil {
			if isDeferredProvideError(err) {
				log.Printf("Deferring DHT provide for %s until connected to peers", cidStr)
				continue
			}
			log.Printf("Failed to provide CID %s: %v", cidStr, err)
			continue
		}

		n.ProvidedCIDs[cidStr] = struct{}{}
		log.Printf("Provided manifest swarm %s to DHT", cidStr)
	}

	for cidStr := range n.ProvidedCIDs {
		if _, stillPresent := current[cidStr]; !stillPresent {
			delete(n.ProvidedCIDs, cidStr)
		}
	}
}

// detects and ignores harmless early DHT errors when no peers are connected yet.
func isDeferredProvideError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "failed to find any peer in table") ||
		strings.Contains(msg, "no peer in table")
}
