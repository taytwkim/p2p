# Peer-to-Peer File Sharing Network

A small working prototype of a P2P file sharing network.

## Overview

The network operates on two primary peer protocols:

- **Transfer Protocol (`/p2pfs/get/1.0.0`)**: Stream-based protocol handling file downloads.

- **Index Protocol (`/p2pfs/index/1.0.0`)**: Stream-based request protocol for listing files, checking exact objects, and exchanging piece availability bitfields.

It uses `libp2p`'s **Kademlia DHT** for swarm discovery. A downloadable file is identified by its manifest CID, and that same manifest CID acts as the file's swarm identifier. Every piece is still identified by a CID derived from its raw bytes, but piece ownership is exchanged directly between peers instead of being looked up through the DHT one piece at a time.

Nodes periodically scan their local export directory and register themselves as providers for newly discovered manifests. Other peers use those manifest provider records to find swarm participants, ask each participant for a piece availability bitfield, and then fetch pieces directly from selected peers.

Peers also participate while a download is still in progress. Once a peer has verified a manifest, it joins that manifest swarm; each verified piece immediately updates the peer's availability bitfield and can be served to other peers.

## Swarms and Pieces

Each user-visible file is represented by:

- a **file CID**, computed from the complete file bytes
- one or more **piece CIDs**, computed from fixed-size byte ranges
- a **manifest CID**, computed from a JSON manifest that names the file and lists its pieces in order

The manifest CID is the CID to share and fetch. Fetching a manifest downloads the manifest JSON, downloads its pieces in parallel, verifies each piece, reconstructs the file, and verifies the final file CID stored inside the manifest.

During fetch, peers exchange a prototype bitfield over the Index protocol:

```json
{
  "manifestCid": "bafy...",
  "availability": [true, false, true]
}
```

Each boolean position corresponds to the piece at the same index in the manifest. For example, `[true, false, true]` means the peer has pieces `0` and `2`, but not piece `1`.

For the current demo-friendly implementation, the piece size is intentionally small: 5 bytes. A file containing:

```text
AAAA
BBBB
CCCC
```

is split into three pieces: `AAAA\n`, `BBBB\n`, and `CCCC\n`.

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
- `whohas <manifest-cid>`: Query the DHT for peers participating in a manifest swarm.
- `fetch <manifest-cid> [peer|alias]`: Download a file by manifest CID, optionally from a specific peer.
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

- `whohas`: Ask the local daemon to query the DHT for peers participating in a manifest swarm.

```bash
./p2pfs whohas <MANIFEST_CID>
```

- `fetch`: Tell daemon to download a file by manifest CID into its local `export_dir`.

```bash
./p2pfs fetch <MANIFEST_CID>
```

- `list`: Connect to a remote peer explicitly and use the Index protocol to verify what they are serving, including filename, CID, and size.

```bash
./p2pfs list --peer <REMOTE_MULTIADDR>
```
