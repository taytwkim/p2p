package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

/*
 * runDaemon starts the local node as a daemon.
 *
 * runWhohas, runFetch, and runList are CLI commands we use to issue requests to the
 * local daemon over RPC.
 *
 * The daemon then decides what action to take.
 * If needed, it uses do_X(...) functions to contact another peer,
 * and that peer handles the request in handle_X(...).
 *
 * Example flow:
 * 		1. We issue runFetch on CLI
 * 		2. local daemon receives runFetch and calls doFetch (see rpc.go and transfer.go)
 * 		3. remote peer receives doFetch which is handled by handleTransferStream (see transfer.go)
 */

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "daemon":
		runDaemon(args)
	case "shell":
		runShell(args)
	case "whohas":
		runWhohas(args)
	case "fetch":
		runFetch(args)
	case "list":
		runList(args)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: p2pfs <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  daemon  Run the p2pfs daemon")
	fmt.Println("  shell   Run an interactive p2pfs shell")
	fmt.Println("  whohas  Find who has a specific CID")
	fmt.Println("  fetch   Download content by CID")
	fmt.Println("  list    List files served by a peer with filenames and CIDs")
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	// fs.String(flag, default, helper description)
	listenAddr := fs.String("listen", "/ip4/0.0.0.0/tcp/4001", "Listen multiaddr") // node listens at IP:port
	exportDir := fs.String("export_dir", "./files_to_serve", "Directory to serve files from")
	bootstrapOpt := fs.String("bootstrap", "", "Comma-separated list of bootstrap multiaddrs") // node that can be used to bootstrap the starting node
	rpcOpt := fs.String("rpc", "/tmp/p2pfs.sock", "RPC Unix socket path")                      // rpc socket to issue commands to the running daemon

	fs.Parse(args)

	var bootstrapAddrs []string
	if *bootstrapOpt != "" {
		bootstrapAddrs = strings.Split(*bootstrapOpt, ",")
	}

	log.Printf("Starting daemon...")
	log.Printf("Listen addr: %s", *listenAddr)
	log.Printf("Export dir: %s", *exportDir)
	log.Printf("Bootstrap peers: %v", bootstrapAddrs)

	// Create export_dir if it doesn't exist
	if err := os.MkdirAll(*exportDir, 0755); err != nil {
		log.Fatalf("Failed to create export directory: %v", err)
	}

	// Start node, join discovery
	node, err := NewNode(*listenAddr, *exportDir, *rpcOpt, bootstrapAddrs) // in node.go
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	log.Printf("Daemon running. Press Ctrl+C to exit.")

	// Keep daemon alive by blocking forever, note that NewNode is async
	select {}
}

// ============================================================================
// CLI Commands
// These functions parse CLI input and issue requests to nodes running in the
// background, such as whohas, fetch, and list.
//
// These functions do not talk to other peers directly.
// Instead, they call the RPC client in rpc.go, which forwards the command to
// the node.
//
// See rpc.go, which defines both:
// 1. the CLI-side RPC functions that issue commands, and
// 2. the daemon-side handlers that receive those commands and call functions
//    like doList and doFetch.
// ============================================================================

// node X broadcasts whohas
func runWhohas(args []string) {
	fs := flag.NewFlagSet("whohas", flag.ExitOnError)
	rpcOpt := fs.String("rpc", "/tmp/p2pfs.sock", "RPC Unix socket path")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Usage: p2pfs whohas [--rpc <socket>] <cid>")
		os.Exit(1)
	}

	cid := fs.Arg(0)
	fmt.Printf("Querying who has CID: %s\n", cid)

	// connect to daemon to issue commands
	client := NewClient(*rpcOpt)
	providers, err := client.Whohas(cid)

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	if len(providers) == 0 {
		fmt.Println("No providers found.")
	} else {
		fmt.Printf("Providers for %s:\n", cid)
		for _, p := range providers {
			fmt.Printf("  %s\n", p.PeerID)
		}
	}
}

// download file from peer X
func runFetch(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	fromPeer := fs.String("from", "", "Specific peer ID to fetch from (optional)")
	rpcOpt := fs.String("rpc", "/tmp/p2pfs.sock", "RPC Unix socket path")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Usage: p2pfs fetch <cid> [--from <peer_id>]")
		os.Exit(1)
	}
	cid := fs.Arg(0)

	log.Printf("Fetching CID: %s", cid)
	client := NewClient(*rpcOpt)

	startTime := time.Now()
	err := client.Fetch(cid, *fromPeer)
	if err != nil {
		log.Fatalf("Fetch failed: %v", err)
	}
	log.Printf("Fetch complete in %v", time.Since(startTime))
}

// list the files owned by node X
func runList(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	peerAddr := fs.String("peer", "", "Target peer multiaddr")
	rpcOpt := fs.String("rpc", "/tmp/p2pfs.sock", "RPC Unix socket path")
	fs.Parse(args)

	if *peerAddr == "" {
		fmt.Println("Usage: p2pfs list --peer <multiaddr>")
		os.Exit(1)
	}

	log.Printf("Listing files for %s", *peerAddr)
	client := NewClient(*rpcOpt)
	files, err := client.List(*peerAddr)
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}
	fmt.Println("Files served:")
	for _, f := range files {
		fmt.Printf("  - %s  %s  %d bytes\n", f.Filename, f.CID, f.Size)
	}
}
