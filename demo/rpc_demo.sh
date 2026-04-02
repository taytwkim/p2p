#!/bin/bash

# rpc_demo.sh - A script to set up, run, and clean the P2P file sharing demo using daemon + RPC commands.

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to clear old state
clean() {
    echo -e "${BLUE}Cleaning up old demo state...${NC}"
    killall p2pfs 2>/dev/null || true
    rm -rf peerA_export peerB_export peerC_export
    rm -f peerA.log peerB.log peerC.log
    rm -f /tmp/p2pfsA.sock /tmp/p2pfsB.sock /tmp/p2pfsC.sock
    echo "Done."
}

# Function to setup and start the demo
setup() {
    echo -e "${BLUE}Building p2pfs...${NC}"
    GOCACHE=/tmp/go-build go build -o p2pfs

    echo -e "${BLUE}Setting up directories...${NC}"
    mkdir -p peerA_export peerB_export peerC_export
    echo "Hello from Peer A!" > peerA_export/foo.txt

    echo -e "${GREEN}Starting Peer A (Seed)...${NC}"
    ./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4001 -export_dir ./peerA_export -rpc /tmp/p2pfsA.sock > peerA.log 2>&1 &
    sleep 2

    A_ADDR=$(grep "Listening on: /ip4/127.0.0.1/tcp/4001/p2p/" peerA.log | head -1 | awk '{print $5}')
    echo "Peer A Address: $A_ADDR"

    echo -e "${GREEN}Starting Peer B (Bootstrap node connecting to A)...${NC}"
    ./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4002 -export_dir ./peerB_export -rpc /tmp/p2pfsB.sock -bootstrap "$A_ADDR" > peerB.log 2>&1 &
    sleep 2

    B_ADDR=$(grep "Listening on: /ip4/127.0.0.1/tcp/4002/p2p/" peerB.log | head -1 | awk '{print $5}')
    echo "Peer B Address: $B_ADDR"

    echo -e "${GREEN}Starting Peer C (Leech connecting to B)...${NC}"
    ./p2pfs daemon -listen /ip4/127.0.0.1/tcp/4003 -export_dir ./peerC_export -rpc /tmp/p2pfsC.sock -bootstrap "$B_ADDR" > peerC.log 2>&1 &
    
    echo -e "\n${BLUE}All peers started! Wait a few seconds for the DHT routing tables to warm up (~5-10s)...${NC}"
    echo -e "You can now run commands against Peer C to inspect files, find the CID for foo.txt, and fetch it:"
    echo -e "  ./p2pfs list   --rpc /tmp/p2pfsC.sock --peer <REMOTE_MULTIADDR>"
    echo -e "  ./p2pfs whohas --rpc /tmp/p2pfsC.sock <CID>"
    echo -e "  ./p2pfs fetch  --rpc /tmp/p2pfsC.sock <CID>"
    echo -e "  cat peerC_export/foo.txt\n"
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
        echo "Usage: ./rpc_demo.sh {start|clean|setup}"
        echo "  start : Cleans up old state, builds, and starts the 3-peer network"
        echo "  clean : Kills running peers and removes export directories / logs"
        exit 1
esac
