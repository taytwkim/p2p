package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
 * rpc.go defines the RPC layer between CLI and the local daemon.
 *
 * 1. A CLI command in main.go creates an RPC Client and issues a request.
 * 2. The local daemon receives that request through a daemon-side RPC handler.
 * 3. The handlers are wrappers that trigger doFetch(...), doList(...), ... .
 */

// ============================================================================
// Shared RPC Types
// These structs define the request/response payloads passed between the CLI
// process and the local daemon over the Unix RPC socket.
// ============================================================================

type WhohasArgs struct {
	CID string
}

type WhohasReply struct {
	Providers []ProviderInfo
}

type FetchArgs struct {
	CID    string
	PeerID string
}

type FetchReply struct {
	Success bool
	Events  []string
}

type ListArgs struct {
	TargetAddr string
}

type ListReply struct {
	Files []IndexFile
}

type ProviderInfo struct {
	PeerID   string
	Filename string
	Size     int64
}

// ============================================================================
// Client-Side RPC Wrappers
// These methods are called by the CLI in main.go. They issue local control
// commands to the running daemon over the Unix RPC socket.
// ============================================================================

// Client is the CLI-side RPC wrapper for talking to the local daemon.
type Client struct {
	rpcClient *rpc.Client
}

func NewClient(rpcSocket string) *Client {
	c, err := rpc.DialHTTP("unix", rpcSocket)
	if err != nil {
		log.Fatalf("Daemon not running or could not connect: %v", err)
	}
	return &Client{rpcClient: c}
}

func (c *Client) Whohas(cid string) ([]ProviderInfo, error) {
	args := &WhohasArgs{CID: cid}
	var reply WhohasReply
	err := c.rpcClient.Call("P2PFSAPI.Whohas", args, &reply)
	return reply.Providers, err
}

func (c *Client) Fetch(cid, peerID string) (FetchReply, error) {
	args := &FetchArgs{CID: cid, PeerID: peerID}
	var reply FetchReply
	err := c.rpcClient.Call("P2PFSAPI.Fetch", args, &reply)
	return reply, err
}

func (c *Client) List(targetAddr string) ([]IndexFile, error) {
	args := &ListArgs{TargetAddr: targetAddr}
	var reply ListReply
	err := c.rpcClient.Call("P2PFSAPI.List", args, &reply)
	return reply.Files, err
}

// ============================================================================
// Daemon-Side RPC Handlers
// These methods run inside the local daemon. They receive local control
// commands from the CLI and calls actual logic like doFetch and doList.
// ============================================================================

type P2PFSAPI struct {
	node *Node
}

func (api *P2PFSAPI) Whohas(args *WhohasArgs, reply *WhohasReply) error {
	peers, err := api.node.DHT.FindProviders(context.Background(), args.CID, 20)
	if err != nil {
		return err
	}

	for _, info := range peers {
		reply.Providers = append(reply.Providers, ProviderInfo{
			PeerID: info.ID.String(),
		})
	}
	return nil
}

func (api *P2PFSAPI) Fetch(args *FetchArgs, reply *FetchReply) error {
	status := func(format string, args ...any) {
		reply.Events = append(reply.Events, fmt.Sprintf(format, args...))
	}
	err := api.node.doFetchWithStatus(args.CID, args.PeerID, status)
	if err == nil {
		reply.Success = true
	}
	return err
}

func (api *P2PFSAPI) List(args *ListArgs, reply *ListReply) error {
	files, err := api.node.doList(args.TargetAddr)
	if err == nil {
		reply.Files = files
	}
	return err
}

// ============================================================================
// RPC Server Startup
// This creates the local Unix-socket server that the CLI connects to.
// Each node calls startRPCServer() at startup
// ============================================================================

func (n *Node) startRPCServer() error {
	api := &P2PFSAPI{node: n}
	rpcServer := rpc.NewServer()
	rpcServer.Register(api)

	// Clean up old socket if it exists.
	os.Remove(n.RpcSocket)

	l, err := net.Listen("unix", n.RpcSocket)
	if err != nil {
		return err
	}
	n.rpcListener = l

	go func() {
		defer l.Close()
		http.Serve(l, rpcServer)
	}()

	log.Printf("RPC Server listening on %s", n.RpcSocket)
	return nil
}
