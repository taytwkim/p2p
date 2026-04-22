#!/bin/bash

# rpc_demo.sh - A script to set up, run, and clean the P2P file sharing demo using daemon + RPC commands.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_ROOT/p2pfs"
PEER_A_EXPORT="$REPO_ROOT/peerA_export"
PEER_B_EXPORT="$REPO_ROOT/peerB_export"
PEER_C_EXPORT="$REPO_ROOT/peerC_export"
PEER_A_LOG="$REPO_ROOT/peerA.log"
PEER_B_LOG="$REPO_ROOT/peerB.log"
PEER_C_LOG="$REPO_ROOT/peerC.log"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to clear old state
clean() {
    echo -e "${BLUE}Cleaning up old demo state...${NC}"
    killall p2pfs 2>/dev/null || true
    rm -rf "$PEER_A_EXPORT" "$PEER_B_EXPORT" "$PEER_C_EXPORT"
    rm -f "$PEER_A_LOG" "$PEER_B_LOG" "$PEER_C_LOG"
    rm -f /tmp/p2pfsA.sock /tmp/p2pfsB.sock /tmp/p2pfsC.sock
    echo "Done."
}

# Function to setup and start the demo
setup() {
    echo -e "${BLUE}Building p2pfs...${NC}"
    cd "$REPO_ROOT"
    GOCACHE=/tmp/go-build go build -o "$BINARY"

    echo -e "${BLUE}Setting up directories...${NC}"
    mkdir -p "$PEER_A_EXPORT" "$PEER_B_EXPORT" "$PEER_C_EXPORT"
    echo "Hello from Peer A!" > "$PEER_A_EXPORT/foo.txt"
    printf "AAAA\nBBBB\nCCCC\n" > "$PEER_A_EXPORT/pieces.txt"

    echo -e "${GREEN}Starting Peer A (Seed)...${NC}"
    "$BINARY" daemon -listen /ip4/127.0.0.1/tcp/4001 -export_dir "$PEER_A_EXPORT" -rpc /tmp/p2pfsA.sock > "$PEER_A_LOG" 2>&1 &
    sleep 2

    A_ADDR=$(grep "Listening on: /ip4/127.0.0.1/tcp/4001/p2p/" "$PEER_A_LOG" | head -1 | awk '{print $5}')
    echo "Peer A Address: $A_ADDR"

    echo -e "${GREEN}Starting Peer B (Bootstrap node connecting to A)...${NC}"
    "$BINARY" daemon -listen /ip4/127.0.0.1/tcp/4002 -export_dir "$PEER_B_EXPORT" -rpc /tmp/p2pfsB.sock -bootstrap "$A_ADDR" > "$PEER_B_LOG" 2>&1 &
    sleep 2

    B_ADDR=$(grep "Listening on: /ip4/127.0.0.1/tcp/4002/p2p/" "$PEER_B_LOG" | head -1 | awk '{print $5}')
    echo "Peer B Address: $B_ADDR"

    echo -e "${GREEN}Starting Peer C (Leech connecting to B)...${NC}"
    "$BINARY" daemon -listen /ip4/127.0.0.1/tcp/4003 -export_dir "$PEER_C_EXPORT" -rpc /tmp/p2pfsC.sock -bootstrap "$B_ADDR" > "$PEER_C_LOG" 2>&1 &
    
    echo -e "\n${BLUE}All peers started! Wait a few seconds for the DHT routing tables to warm up (~5-10s)...${NC}"
    echo -e "You can now run commands against Peer C to inspect files, find the manifest CID for foo.txt or pieces.txt, and fetch it:"
    echo -e "  ./p2pfs list   --rpc /tmp/p2pfsC.sock --peer <REMOTE_MULTIADDR>"
    echo -e "  ./p2pfs whohas --rpc /tmp/p2pfsC.sock <MANIFEST_CID>"
    echo -e "  ./p2pfs fetch  --rpc /tmp/p2pfsC.sock <MANIFEST_CID>"
    echo -e "  cat peerC_export/foo.txt\n"
    echo -e "For the readable piece smoke test, fetch pieces.txt and then run:"
    echo -e "  cat peerC_export/pieces.txt\n"
}

case "$1" in
    clean)
        clean
        ;;
    setup)
        setup
        ;;
    start)
        clean
        setup
        ;;
    *)
        echo "Usage: ./demo/rpc_demo.sh {start|clean|setup}"
        echo "  start : Cleans up old state, builds, and starts the 3-peer network"
        echo "  clean : Kills running peers and removes export directories / logs"
        exit 1
esac
