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

// LocalObjectRecord describes one CID-addressed object this node can serve.
// User-visible files are exposed as a manifest plus one object per chunk.
type LocalObjectRecord struct {
	CID         string          // The CID for this object.
	Kind        LocalObjectKind // Whether this record is a manifest or chunk.
	Filename    string          // Human-friendly name.
	Path        string
	Size        int64
	Offset      int64     // For chunks served from a larger file, where the chunk starts.
	Length      int64     // For chunks, how many bytes to serve.
	ManifestCID string    // The manifest CID for the file this object belongs to.
	ChunkCount  int       // Number of chunks in the file.
	Manifest    *Manifest // Parsed manifest object, useful for manifest records.
}

type LocalObjectKind string

const (
	ObjectManifest LocalObjectKind = "manifest"
	ObjectChunk    LocalObjectKind = "chunk"
)

// Node is a local p2p daemon
type Node struct {
	ctx              context.Context // ctx and cancel are used to manage the lifecycle of daemons.
	cancel           context.CancelFunc
	Host             host.Host                    // core engine provided by libp2p, representing your presence on the network.
	ExportDir        string                       // local path to the folder where shared files live.
	RpcSocket        string                       // path to the local Unix Domain Socket used for CLI commands.
	LocalObjects     map[string]LocalObjectRecord // local objects keyed by CID, so content is the identity.
	localObjectsLock sync.RWMutex                 // prevents race conditions when accessing the LocalObjects map.
	DHT              DHTNode                      // Kademlia DHT used for provider registration and lookup.
	ProvidedCIDs     map[string]struct{}          // local CIDs already announced into the DHT (we don't want to announce again).
	providedLock     sync.Mutex
	rpcListener      net.Listener // rpcListener holds the open Unix Domain Socket listener for CLI clients.
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
		ctx:          ctx,
		cancel:       cancel,
		Host:         h,
		ExportDir:    exportDir,
		RpcSocket:    rpcSocket,
		LocalObjects: make(map[string]LocalObjectRecord),
		ProvidedCIDs: make(map[string]struct{}),
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

// Wrapper to call updateLocalObjects periodically
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

	newObjects := make(map[string]LocalObjectRecord)
	for _, f := range files {
		if f.IsDir() || f.Name() == ".p2pfs" || strings.HasSuffix(f.Name(), ".downloading") {
			continue
		}
		path := filepath.Join(n.ExportDir, f.Name())

		manifest, manifestBytes, manifestCID, err := BuildManifest(path, f.Name(), defaultChunkSize)
		if err != nil {
			log.Printf("Error building manifest for %s: %v", f.Name(), err)
			continue
		}
		manifestPath := manifestStoragePath(n.ExportDir, manifestCID)
		if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
			log.Printf("Error writing manifest for %s: %v", f.Name(), err)
			continue
		}

		newObjects[manifestCID] = LocalObjectRecord{
			CID:         manifestCID,
			Kind:        ObjectManifest,
			Filename:    f.Name(),
			Path:        manifestPath,
			Size:        int64(len(manifestBytes)),
			ManifestCID: manifestCID,
			ChunkCount:  len(manifest.Chunks),
			Manifest:    manifest,
		}

		for _, chunk := range manifest.Chunks {
			newObjects[chunk.CID] = LocalObjectRecord{
				CID:         chunk.CID,
				Kind:        ObjectChunk,
				Filename:    fmt.Sprintf("%s.chunk-%d", f.Name(), chunk.Index),
				Path:        path,
				Size:        chunk.Size,
				Offset:      chunk.Offset,
				Length:      chunk.Size,
				ManifestCID: manifestCID,
			}
		}
	}

	n.localObjectsLock.Lock()
	n.LocalObjects = newObjects
	n.localObjectsLock.Unlock()

	n.provideNewObjectCIDs(newObjects)
}

// helper that creates .p2pfs/manifests and .p2pfs/chunks
func ensureP2PFSDirs(exportDir string) error {
	if err := os.MkdirAll(filepath.Join(exportDir, ".p2pfs", "manifests"), 0755); err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(exportDir, ".p2pfs", "chunks"), 0755)
}

// helper that returns where a manifest JSON file should be stored.
func manifestStoragePath(exportDir, manifestCID string) string {
	return filepath.Join(exportDir, ".p2pfs", "manifests", manifestCID+".json")
}

// returns where a downloaded chunk should be cached.
func chunkStoragePath(exportDir, chunkCID string) string {
	return filepath.Join(exportDir, ".p2pfs", "chunks", chunkCID)
}

// announces manifest and chunk CIDs to the DHT.
func (n *Node) provideNewObjectCIDs(objects map[string]LocalObjectRecord) {
	n.providedLock.Lock()
	defer n.providedLock.Unlock()

	current := make(map[string]struct{}, len(objects))
	for cidStr := range objects {
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
		log.Printf("Provided CID %s to DHT", cidStr)
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
