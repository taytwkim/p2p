package main

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// DHT APIs
type DHTNode interface {
	Bootstrap(ctx context.Context) error
	Provide(ctx context.Context, cidStr string, announce bool) error
	FindProviders(ctx context.Context, cidStr string, limit int) ([]peer.AddrInfo, error)
	Close() error
}

// Wrapper around the actual DHT object
type KadDHT struct {
	inner *dht.IpfsDHT
}

func NewDHTNode(ctx context.Context, h host.Host, bootstrapAddrs []string) (DHTNode, error) {
	options := []dht.Option{
		dht.Mode(dht.ModeServer),
	}

	if len(bootstrapAddrs) > 0 {
		infos, err := parseBootstrapAddrInfos(bootstrapAddrs)
		if err != nil {
			return nil, err
		}
		options = append(options, dht.BootstrapPeers(infos...))
	}

	inner, err := dht.New(ctx, h, options...)
	if err != nil {
		return nil, err
	}

	return &KadDHT{inner: inner}, nil
}

// tells the local DHT instance to begin participating in the DHT network
func (k *KadDHT) Bootstrap(ctx context.Context) error {
	return k.inner.Bootstrap(ctx)
}

// If Peer A participates in the swarm for manifest X, Peer A calls Provide(X, true).
func (k *KadDHT) Provide(ctx context.Context, cidStr string, announce bool) error {
	parsed, err := cid.Parse(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}
	return k.inner.Provide(ctx, parsed, announce)
}

// If Peer B wants manifest X, it calls FindProviders(X) to discover swarm peers.
func (k *KadDHT) FindProviders(ctx context.Context, cidStr string, limit int) ([]peer.AddrInfo, error) {
	parsed, err := cid.Parse(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID %q: %w", cidStr, err)
	}

	if limit <= 0 {
		return k.inner.FindProviders(ctx, parsed)
	}

	var providers []peer.AddrInfo
	for info := range k.inner.FindProvidersAsync(ctx, parsed, limit) {
		providers = append(providers, info)
	}
	return providers, nil
}

func (k *KadDHT) Close() error {
	return k.inner.Close()
}

// convert string -> multiaddr format for DHT
func parseBootstrapAddrInfos(addrs []string) ([]peer.AddrInfo, error) {
	var infos []peer.AddrInfo
	for _, addrStr := range addrs {
		if addrStr == "" {
			continue
		}

		maddr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap address %s: %w", addrStr, err)
		}

		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap info %s: %w", addrStr, err)
		}

		infos = append(infos, *info)
	}
	return infos, nil
}
