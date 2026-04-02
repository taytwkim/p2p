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

type LocalFileRecord struct {
	CID      string
	Filename string
	Path     string
	Size     int64
}

// Node represents the local p2pfs daemon
type Node struct {
	ctx            context.Context // ctx and cancel are used to manage the lifecycle of daemons.
	cancel         context.CancelFunc
	Host           host.Host                  // core engine provided by libp2p, representing your presence on the network.
	ExportDir      string                     // local path to the folder where shared files live.
	RpcSocket      string                     // path to the local Unix Domain Socket used for CLI commands.
	LocalFiles     map[string]LocalFileRecord // cache of local files, keyed by CID so content is the identity.
	localFilesLock sync.RWMutex               // prevents race conditions when accessing the LocalFiles map.
	DHT            DHTNode                    // Kademlia DHT used for provider registration and lookup.
	ProvidedCIDs   map[string]struct{}        // local CIDs already announced into the DHT (we don't want to announce again).
	providedLock   sync.Mutex
	rpcListener    net.Listener // rpcListener holds the open Unix Domain Socket listener for CLI clients.
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
		LocalFiles:   make(map[string]LocalFileRecord),
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
	go n.scanLocalFiles()

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

		// take IP and convert to protocol-agnostic multiaddr format
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

// Wrapper to call updateLocalFiles periodically
func (n *Node) scanLocalFiles() {
	// We poll because we want to check whether the user has uploaded a new file in export_dir
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run once immediately
	n.updateLocalFiles()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.updateLocalFiles()
		}
	}
}

func (n *Node) updateLocalFiles() {
	files, err := os.ReadDir(n.ExportDir)
	if err != nil {
		log.Printf("Error reading export dir: %v", err)
		return
	}

	newFiles := make(map[string]LocalFileRecord)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		info, err := f.Info()
		if err != nil {
			continue
		}

		path := filepath.Join(n.ExportDir, f.Name())
		cid, err := ComputeCID(path)
		if err != nil {
			log.Printf("Error computing CID for %s: %v", f.Name(), err)
			continue
		}

		newFiles[cid] = LocalFileRecord{
			CID:      cid,
			Filename: f.Name(),
			Path:     path,
			Size:     info.Size(),
		}
	}

	n.localFilesLock.Lock()
	n.LocalFiles = newFiles
	n.localFilesLock.Unlock()

	n.provideNewCIDs(newFiles)
}

func (n *Node) provideNewCIDs(files map[string]LocalFileRecord) {
	n.providedLock.Lock()
	defer n.providedLock.Unlock()

	current := make(map[string]struct{}, len(files))
	for cidStr := range files {
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

func isDeferredProvideError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "failed to find any peer in table") ||
		strings.Contains(msg, "no peer in table")
}
