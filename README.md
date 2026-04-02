# Peer-to-Peer File Sharing

The project is still in-progress.

## Overview

The project currently includes two primary protocols:

- **Transfer Protocol (`/p2pfs/get/1.0.0`)**: Stream-based protocol handling file downloads.

- **Index Protocol (`/p2pfs/index/1.0.0`)**: Stream-based request protocol allowing peers to manually verify what files a target peer is serving.

It uses libp2p's **Kademlia DHT** for content routing. Each file is identified by a CID derived from its raw bytes. Nodes periodically scan their local export directory and register themselves as providers for any newly discovered CIDs, letting other peers locate content on demand.

## Getting Started

Install dependencies and compile:

```bash
go mod tidy
go build -o p2pfs
```

The `p2pfs` binary supports two usage patterns:

- **Interactive shell mode**: Start a node in the foreground and type commands directly into a REPL.
- **Daemon + RPC mode**: Start a node in the background and control it with stateless CLI commands over a local UNIX RPC socket.

## Usage Mode 1: Interactive Shell

Run a node in the foreground:

```bash
./p2pfs shell --listen /ip4/127.0.0.1/tcp/4001 --export_dir ./my_files --name peerA
```

To join an existing network, add `--bootstrap`:

```bash
./p2pfs shell --listen /ip4/127.0.0.1/tcp/4002 --export_dir ./my_files --name peerB --bootstrap <P2P_MULTIADDR_FROM_SEED>
```

### Interactive Commands

- `help`: Show available shell commands.
- `id`: Show this node's peer ID and listen addresses.
- `files`: Show local files discovered in `export_dir`.
- `whohas <cid>`: Query the DHT for peers that provide a CID.
- `fetch <cid> [peer|alias]`: Download a CID, optionally from a specific peer.
- `list <multiaddr|alias>`: Ask a specific peer for the files it is serving.
- `alias <name> <target>`: Save a short alias for a peer ID or full multiaddr.
- `aliases`: Show configured aliases.
- `unalias <name>`: Remove an alias.
- `echo <text> > <filename>`: Write a file into `export_dir` and rescan immediately.
- `rescan`: Rescan `export_dir` immediately.
- `log`: Show buffered background logs.
- `log clear`: Clear buffered background logs.
- `clear`: Clear the terminal screen.
- `exit`: Quit the interactive shell.

## Usage Mode 2: Daemon + RPC CLI

### Start a Daemon

A node can share files in its `export_dir`.

```bash
./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4001 -export_dir ./my_files
```

### Bootstrapping

To bootstrap a new daemon, pass a comma-separated list of known `/ip4/.../p2p/<PeerID>` multiaddresses to the `-bootstrap` flag.

```bash
./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4002 -export_dir ./my_files -bootstrap <P2P_MULTIADDR_FROM_SEED>
```

### CLI Commands

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
