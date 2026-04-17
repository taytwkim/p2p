package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/chzyer/readline"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// This file implements an interactive shell used to control a p2p daemon
// To start a node in interactive mode, run ./p2pfs shell ... instead of ./p2pfs daemon ...

const (
	colorReset  = "\033[0m"
	colorPrompt = "\033[36m"
	colorInfo   = "\033[32m"
	colorWarn   = "\033[33m"
	colorError  = "\033[31m"
)

type shellSession struct {
	node    *Node
	aliases map[string]string
	name    string
	rl      *readline.Instance
	logs    []string
	logMu   sync.Mutex
}

func runShell(args []string) {
	fs := flag.NewFlagSet("shell", flag.ExitOnError)
	listenAddr := fs.String("listen", "/ip4/0.0.0.0/tcp/4001", "Listen multiaddr")
	exportDir := fs.String("export_dir", "./files_to_serve", "Directory to serve files from")
	bootstrapOpt := fs.String("bootstrap", "", "Comma-separated list of bootstrap multiaddrs")
	rpcOpt := fs.String("rpc", "", "Optional RPC Unix socket path")
	nameOpt := fs.String("name", "", "Optional shell alias for this peer (for prompt and display)")
	fs.Parse(args)

	var bootstrapAddrs []string
	if *bootstrapOpt != "" {
		bootstrapAddrs = strings.Split(*bootstrapOpt, ",")
	}

	if err := os.MkdirAll(*exportDir, 0755); err != nil {
		log.Fatalf("Failed to create export directory: %v", err)
	}

	node, err := NewNode(*listenAddr, *exportDir, *rpcOpt, bootstrapAddrs)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	session := &shellSession{
		node:    node,
		aliases: make(map[string]string),
		name:    *nameOpt,
	}
	if session.name != "" {
		session.aliases[session.name] = shellSelfAliasTarget(node)
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          session.prompt(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		HistoryLimit:    200,
	})
	if err != nil {
		log.Fatalf("Failed to initialize interactive shell: %v", err)
	}
	defer rl.Close()
	session.rl = rl

	log.SetOutput(&shellLogWriter{session: session})

	session.printInfo("Interactive shell ready for peer %s", node.Host.ID())
	if session.name != "" {
		session.printInfo("Shell alias: %s", session.name)
	}
	session.printInfo("Export dir: %s", *exportDir)
	session.printInfo("Type 'help' for commands, or Ctrl+C / 'exit' to quit.")

	for {
		rl.SetPrompt(session.prompt())
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			fmt.Println()
			return
		}
		if err == io.EOF {
			fmt.Println()
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		rl.SaveHistory(line)
		if err := session.runCommand(line); err != nil {
			session.printError("%v", err)
		}
	}
}

func (s *shellSession) prompt() string {
	label := "p2pfs"
	if s.name != "" {
		label = s.name
	}
	return fmt.Sprintf("%s%s>%s ", colorPrompt, label, colorReset)
}

func (s *shellSession) runCommand(line string) error {
	if strings.HasPrefix(line, "echo ") {
		return s.runEchoCommand(line)
	}
	if strings.HasPrefix(line, "dump ") {
		return s.runDumpCommand(line)
	}

	args := strings.Fields(line)
	if len(args) == 0 {
		return nil
	}

	switch args[0] {
	case "help":
		printShellHelp()
	case "id":
		s.printInfo("Peer ID: %s", s.node.Host.ID())
		if s.name != "" {
			s.printInfo("Alias: %s", s.name)
		}
		fmt.Println("Addresses:")
		for _, addr := range s.node.Host.Addrs() {
			fmt.Printf("  %s/p2p/%s\n", addr, s.node.Host.ID())
		}
	case "files":
		s.node.localObjectsLock.RLock()
		files := make([]LocalObjectRecord, 0, len(s.node.LocalObjects))
		for _, f := range s.node.LocalObjects {
			if f.Kind != ObjectManifest {
				continue
			}
			files = append(files, f)
		}
		s.node.localObjectsLock.RUnlock()

		sort.Slice(files, func(i, j int) bool {
			return files[i].Filename < files[j].Filename
		})

		if len(files) == 0 {
			s.printWarn("No local files.")
			return nil
		}

		fmt.Println("Local files:")
		for _, f := range files {
			fileSize := f.Size
			if f.Manifest != nil {
				fileSize = f.Manifest.FileSize
			}
			fmt.Printf("  %s\n", f.Filename)
			fmt.Printf("    manifest: %s\n", f.CID)
			fmt.Printf("    size:     %d bytes\n", fileSize)
			fmt.Printf("    chunks:   %d\n", f.ChunkCount)
		}
	case "whohas":
		if len(args) != 2 {
			return fmt.Errorf("usage: whohas <cid>")
		}
		providers, err := s.node.DHT.FindProviders(context.Background(), args[1], 20)
		if err != nil {
			return err
		}
		if len(providers) == 0 {
			s.printWarn("No providers found.")
			return nil
		}
		fmt.Println("Providers:")
		for _, info := range providers {
			fmt.Printf("  %s%s\n", s.aliasLabel(info.ID.String()), info.ID)
		}
	case "fetch":
		if len(args) < 2 || len(args) > 3 {
			return fmt.Errorf("usage: fetch <manifest-cid> [peer_id_or_alias]")
		}
		peerID := ""
		if len(args) == 3 {
			resolvedPeerID, err := s.resolvePeerID(args[2])
			if err != nil {
				return err
			}
			peerID = resolvedPeerID
		}
		return s.runFetch(args[1], peerID)
	case "list":
		if len(args) != 2 {
			return fmt.Errorf("usage: list <peer_multiaddr_or_alias>")
		}
		target := s.resolveAlias(args[1])
		files, err := s.node.doList(target)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			s.printWarn("Remote peer is serving no files.")
			return nil
		}
		fmt.Println("Remote files:")
		for _, f := range files {
			fmt.Printf("  %s\n", f.Filename)
			fmt.Printf("    manifest: %s\n", f.ManifestCID)
			fmt.Printf("    size:     %d bytes\n", f.Size)
			fmt.Printf("    chunks:   %d\n", f.ChunkCount)
		}
	case "alias":
		if len(args) != 3 {
			return fmt.Errorf("usage: alias <name> <peer_id_or_multiaddr>")
		}
		name := args[1]
		s.aliases[name] = args[2]
		s.printInfo("Alias added: %s -> %s", name, args[2])
	case "aliases":
		if len(s.aliases) == 0 {
			s.printWarn("No aliases configured.")
			return nil
		}
		names := make([]string, 0, len(s.aliases))
		for name := range s.aliases {
			names = append(names, name)
		}
		sort.Strings(names)
		fmt.Println("Aliases:")
		for _, name := range names {
			fmt.Printf("  %s -> %s\n", name, s.aliases[name])
		}
	case "unalias":
		if len(args) != 2 {
			return fmt.Errorf("usage: unalias <name>")
		}
		delete(s.aliases, args[1])
		s.printInfo("Alias removed: %s", args[1])
	case "log":
		return s.runLogCommand(args[1:])
	case "clear":
		fmt.Fprint(s.rl.Stdout(), "\033[H\033[2J")
		s.rl.Refresh()
	case "rescan":
		s.node.updateLocalObjects()
		s.printInfo("Rescanned %s", s.node.ExportDir)
	case "exit", "quit":
		os.Exit(0)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}

	return nil
}

func (s *shellSession) resolveAlias(value string) string {
	if resolved, ok := s.aliases[value]; ok {
		return resolved
	}
	return value
}

func (s *shellSession) aliasLabel(value string) string {
	for name, target := range s.aliases {
		if target == value {
			return name + " "
		}
		if peerID, ok := peerIDFromAliasTarget(target); ok && peerID == value {
			return name + " "
		}
	}
	return ""
}

func (s *shellSession) resolvePeerID(value string) (string, error) {
	resolved := s.resolveAlias(value)
	if _, err := peer.Decode(resolved); err == nil {
		return resolved, nil
	}
	if peerID, ok := peerIDFromAliasTarget(resolved); ok {
		return peerID, nil
	}
	return "", fmt.Errorf("expected peer ID or alias pointing to a peer, got %q", value)
}

func peerIDFromAliasTarget(target string) (string, bool) {
	maddr, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		return "", false
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", false
	}
	return info.ID.String(), true
}

func (s *shellSession) printInfo(format string, args ...any) {
	fmt.Fprintf(s.rl.Stdout(), colorInfo+format+colorReset+"\n", args...)
}

func (s *shellSession) printWarn(format string, args ...any) {
	fmt.Fprintf(s.rl.Stdout(), colorWarn+format+colorReset+"\n", args...)
}

func (s *shellSession) printError(format string, args ...any) {
	fmt.Fprintf(s.rl.Stdout(), colorError+format+colorReset+"\n", args...)
}

func (s *shellSession) runFetch(cid, peerID string) error {
	status := func(format string, args ...any) {
		fmt.Fprintf(s.rl.Stdout(), colorInfo+format+colorReset+"\n", args...)
	}
	return s.node.doFetchWithStatus(cid, peerID, status)
}

func (s *shellSession) runLogCommand(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("usage: log [clear]")
	}
	if len(args) == 1 {
		if args[0] != "clear" {
			return fmt.Errorf("usage: log [clear]")
		}
		s.logMu.Lock()
		s.logs = nil
		s.logMu.Unlock()
		s.printInfo("Buffered logs cleared.")
		return nil
	}

	s.logMu.Lock()
	logs := append([]string(nil), s.logs...)
	s.logMu.Unlock()

	if len(logs) == 0 {
		s.printWarn("No buffered logs.")
		return nil
	}

	fmt.Println("Buffered logs:")
	for _, entry := range logs {
		fmt.Fprintln(s.rl.Stdout(), entry)
	}
	return nil
}

func (s *shellSession) runEchoCommand(line string) error {
	body := strings.TrimSpace(strings.TrimPrefix(line, "echo"))
	if body == "" {
		return fmt.Errorf("usage: echo <text> > <filename>")
	}

	parts := strings.SplitN(body, ">", 2)
	if len(parts) != 2 {
		return fmt.Errorf("usage: echo <text> > <filename>")
	}

	content := strings.TrimSpace(parts[0])
	filename := strings.TrimSpace(parts[1])
	if content == "" || filename == "" {
		return fmt.Errorf("usage: echo <text> > <filename>")
	}
	if strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		return fmt.Errorf("filename must stay within export_dir")
	}

	if unquoted, err := strconv.Unquote(content); err == nil {
		content = unquoted
	}
	if unquoted, err := strconv.Unquote(filename); err == nil {
		filename = unquoted
	}

	path := filepath.Join(s.node.ExportDir, filename)
	if err := os.WriteFile(path, []byte(content+"\n"), 0644); err != nil {
		return err
	}

	s.node.updateLocalObjects()
	s.printInfo("Wrote %s", filename)
	return nil
}

func (s *shellSession) runDumpCommand(line string) error {
	body := strings.TrimSpace(strings.TrimPrefix(line, "dump"))
	if body == "" {
		return fmt.Errorf("usage: dump <bytes> > <filename>")
	}

	parts := strings.SplitN(body, ">", 2)
	if len(parts) != 2 {
		return fmt.Errorf("usage: dump <bytes> > <filename>")
	}

	sizeText := strings.TrimSpace(parts[0])
	filename := strings.TrimSpace(parts[1])
	if sizeText == "" || filename == "" {
		return fmt.Errorf("usage: dump <bytes> > <filename>")
	}
	if strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		return fmt.Errorf("filename must stay within export_dir")
	}
	if unquoted, err := strconv.Unquote(filename); err == nil {
		filename = unquoted
	}

	size, err := strconv.Atoi(sizeText)
	if err != nil || size < 0 {
		return fmt.Errorf("byte count must be a non-negative integer")
	}

	content, err := randomPrintableBytes(size)
	if err != nil {
		return err
	}

	path := filepath.Join(s.node.ExportDir, filename)
	if err := os.WriteFile(path, content, 0644); err != nil {
		return err
	}

	s.node.updateLocalObjects()
	s.printInfo("Wrote %d bytes to %s", size, filename)
	return nil
}

func randomPrintableBytes(size int) ([]byte, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	if size == 0 {
		return []byte{}, nil
	}

	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}

	out := make([]byte, size)
	for i, b := range raw {
		out[i] = alphabet[int(b)%len(alphabet)]
	}
	return out, nil
}

type shellLogWriter struct {
	session *shellSession
}

func (w *shellLogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	if msg == "" {
		return len(p), nil
	}
	w.session.logMu.Lock()
	w.session.logs = append(w.session.logs, msg)
	if len(w.session.logs) > 500 {
		w.session.logs = w.session.logs[len(w.session.logs)-500:]
	}
	w.session.logMu.Unlock()
	return len(p), nil
}

func shellSelfAliasTarget(node *Node) string {
	if len(node.Host.Addrs()) > 0 {
		return fmt.Sprintf("%s/p2p/%s", node.Host.Addrs()[0], node.Host.ID())
	}
	return node.Host.ID().String()
}

func printShellHelp() {
	fmt.Println("Commands:")
	fmt.Println("  help                         Show this help")
	fmt.Println("  id                           Show peer ID and listen addresses")
	fmt.Println("  files                        Show local files discovered in export_dir")
	fmt.Println("  whohas <cid>                 Query the DHT for providers of a CID")
	fmt.Println("  fetch <manifest-cid> [peer|alias] Fetch a file by manifest CID")
	fmt.Println("  list <multiaddr|alias>       List the files served by a remote peer")
	fmt.Println("  alias <name> <target>        Save a short alias for a peer ID or multiaddr")
	fmt.Println("  aliases                      Show configured aliases")
	fmt.Println("  unalias <name>               Remove an alias")
	fmt.Println("  echo <text> > <filename>     Write a file into export_dir and rescan")
	fmt.Println("  dump <bytes> > <filename>    Write N random printable bytes and rescan")
	fmt.Println("  rescan                       Rescan export_dir immediately")
	fmt.Println("  log [clear]                  Show or clear buffered background logs")
	fmt.Println("  clear                        Clear the terminal screen")
	fmt.Println("  exit                         Quit the interactive shell")
}
