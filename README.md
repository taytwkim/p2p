# Peer-to-Peer File Sharing Network

A small working prototype of a P2P file sharing network.

## Overview

The network operates on two primary protocols:

- **Transfer Protocol (`/p2pfs/get/1.0.0`)**: Stream-based protocol handling file downloads.

- **Index Protocol (`/p2pfs/index/1.0.0`)**: Stream-based request protocol allowing peers to manually verify what files a target peer is serving.

It uses `libp2p`'s **Kademlia DHT** for content routing. Each file is identified by a CID derived from its content (raw bytes). 

Nodes periodically scan their local export directory and register themselves as providers for newly discovered manifests, full files, and chunks, letting other peers locate content on demand.

## Chunked Files

Each user-visible file is represented by:

- a **file CID**, computed from the complete file bytes
- one or more **chunk CIDs**, computed from fixed-size byte ranges
- a **manifest CID**, computed from a JSON manifest that names the file and lists its chunks in order

The manifest CID is the main CID to share and fetch. Fetching a manifest downloads the manifest JSON, downloads its chunks in parallel, verifies each chunk, reconstructs the file, and verifies the final file CID.

For the current demo-friendly implementation, the chunk size is intentionally small: 5 bytes. A file containing:

```text
AAAA
BBBB
CCCC
```

is split into three chunks: `AAAA\n`, `BBBB\n`, and `CCCC\n`.

## Getting Started

Install dependencies and compile:

```bash
go mod tidy
go build -o p2pfs
```

The `p2pfs` binary supports two usage patterns:

- **Interactive shell**: Start a node in the foreground and type commands directly into a REPL.
- **Daemon + RPC**: Start a node in the background and control it by issuing stateless requests over a local UNIX RPC socket.

### Mode 1: Interactive Shell

Run a node in the foreground:

```bash
./p2pfs shell --listen /ip4/127.0.0.1/tcp/4001 --export_dir ./my_files --name peerA
```

To join an existing network, add `--bootstrap`:

```bash
./p2pfs shell --listen /ip4/127.0.0.1/tcp/4002 --export_dir ./my_files --name peerB --bootstrap <P2P_MULTIADDR_FROM_SEED>
```

**Interactive Commands**

- `help`: Show available shell commands.
- `id`: Show this node's peer ID and listen addresses.
- `files`: Show local files discovered in `export_dir`.
- `whohas <cid>`: Query the DHT for peers that provide a CID.
- `fetch <cid> [peer|alias]`: Download a CID, optionally from a specific peer. Use the manifest CID for chunked downloads.
- `list <multiaddr|alias>`: Ask a specific peer for the files it is serving.
- `alias <name> <target>`: Save a short alias for a peer ID or full multiaddr.
- `aliases`: Show configured aliases.
- `unalias <name>`: Remove an alias.
- `echo <text> > <filename>`: Write a file into `export_dir` and rescan immediately.
- `dump <# bytes> > <filename>`: Dump N random bytes to a file.
- `rescan`: Rescan `export_dir` immediately.
- `log`: Show buffered background logs.
- `log clear`: Clear buffered background logs.
- `clear`: Clear the terminal screen.
- `exit`: Quit the interactive shell.

### Mode 2: Daemon + Control Over RPC

**Start a Daemon**

Start a node in the background.

```bash
./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4001 -export_dir ./my_files
```

**Bootstrapping**

To bootstrap a new daemon, pass a comma-separated list of known `/ip4/.../p2p/<PeerID>` multiaddresses to the `-bootstrap` flag.

```bash
./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4002 -export_dir ./my_files -bootstrap <P2P_MULTIADDR_FROM_SEED>
```

**CLI Commands**

Once the daemon is up and connected to the DHT through its bootstrap peers, control it with the CLI:

- `whohas`: Ask the local daemon to query the DHT for peers that provide a specific CID.

```bash
./p2pfs whohas <CID>
```

- `fetch`: Tell daemon to download content by CID from the network into its local `export_dir`.

```bash
./p2pfs fetch <CID>
```

- `list`: Connect to a remote peer explicitly and use the Index protocol to verify what they are serving, including filename, CID, and size.

```bash
./p2pfs list --peer <REMOTE_MULTIADDR>
```
