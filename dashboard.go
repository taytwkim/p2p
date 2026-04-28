package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type dashboardPeerConfig struct {
	ID        string
	Name      string
	Socket    string
	ExportDir string
	LogPath   string
}

type dashboardFetchRecord struct {
	ManifestCID string
	TargetPeer  string
	StartedAt   time.Time
	FinishedAt  time.Time
	LastError   string
}

type dashboardServer struct {
	repoRoot string
	uiDir    string
	peers    []dashboardPeerConfig

	mu      sync.Mutex
	fetches map[string]*dashboardFetchRecord
}

type dashboardSummary struct {
	PeersOnline int `json:"peersOnline"`
	SeededFiles int `json:"seededFiles"`
}

type dashboardPeerStats struct {
	Files           int `json:"files"`
	Pieces          int `json:"pieces"`
	ActiveTransfers int `json:"activeTransfers"`
}

type dashboardPeerFile struct {
	Filename    string `json:"filename"`
	ManifestCID string `json:"manifestCid"`
	Size        int64  `json:"size"`
	PieceCount  int    `json:"pieceCount"`
}

type dashboardPeer struct {
	ID        string              `json:"id"`
	Name      string              `json:"name"`
	Online    bool                `json:"online"`
	Status    string              `json:"status"`
	Socket    string              `json:"socket"`
	ExportDir string              `json:"exportDir"`
	LogPath   string              `json:"logPath"`
	Stats     dashboardPeerStats  `json:"stats"`
	Files     []dashboardPeerFile `json:"files"`
}

type dashboardFile struct {
	Filename            string   `json:"filename"`
	ManifestCID         string   `json:"manifestCid"`
	Size                int64    `json:"size"`
	PieceCount          int      `json:"pieceCount"`
	Providers           []string `json:"providers"`
	DefaultTargetPeerID string   `json:"defaultTargetPeerId"`
}

type dashboardTransferPiece struct {
	Index int    `json:"index"`
	State string `json:"state"`
	Owner string `json:"owner"`
}

type dashboardTransfer struct {
	ManifestCID      string                   `json:"manifestCid"`
	Filename         string                   `json:"filename"`
	Downloader       string                   `json:"downloader"`
	CompletedPieces  int                      `json:"completedPieces"`
	TotalPieces      int                      `json:"totalPieces"`
	ProvidersEngaged int                      `json:"providersEngaged"`
	Throughput       string                   `json:"throughput"`
	Pieces           []dashboardTransferPiece `json:"pieces"`
}

type dashboardStateResponse struct {
	Summary        dashboardSummary    `json:"summary"`
	Config         dashboardConfig     `json:"config"`
	Peers          []dashboardPeer     `json:"peers"`
	Files          []dashboardFile     `json:"files"`
	ActiveTransfer *dashboardTransfer  `json:"activeTransfer,omitempty"`
}

type dashboardConfig struct {
	IdlePollMs     int `json:"idlePollMs"`
	ActivePollMs   int `json:"activePollMs"`
	TransferPollMs int `json:"transferPollMs"`
}

type fetchRequest struct {
	ManifestCID  string `json:"manifestCid"`
	TargetPeerID string `json:"targetPeerId"`
}

type peerRuntimeState struct {
	cfg      dashboardPeerConfig
	reply    StateReply
	err      error
	peerID   string
	peerName string
}

func runDashboard(args []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	fs.Parse(args)

	repoRoot, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to determine working directory: %v", err)
	}
	localCfg := loadLocalDemoConfig(repoRoot)

	server := &dashboardServer{
		repoRoot: repoRoot,
		uiDir:    filepath.Join(repoRoot, localCfg.DashboardUIRel),
		peers: []dashboardPeerConfig{
			{ID: "peerA", Name: "Peer A", Socket: "/tmp/tinytorrentA.sock", ExportDir: filepath.Join(repoRoot, "peerA_export"), LogPath: filepath.Join(repoRoot, "peerA.log")},
			{ID: "peerB", Name: "Peer B", Socket: "/tmp/tinytorrentB.sock", ExportDir: filepath.Join(repoRoot, "peerB_export"), LogPath: filepath.Join(repoRoot, "peerB.log")},
			{ID: "peerC", Name: "Peer C", Socket: "/tmp/tinytorrentC.sock", ExportDir: filepath.Join(repoRoot, "peerC_export"), LogPath: filepath.Join(repoRoot, "peerC.log")},
		},
		fetches: make(map[string]*dashboardFetchRecord),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", server.handleState)
	mux.HandleFunc("/api/transfer", server.handleTransfer)
	mux.HandleFunc("/api/fetch", server.handleFetch)
	mux.HandleFunc("/api/peers/", server.handlePeerRoutes)
	mux.Handle("/", http.FileServer(http.Dir(server.uiDir)))

	log.Printf("Dashboard listening on http://%s", *addr)
	log.Printf("Start the demo harness with ./demo/local/local_demo.sh start before using the dashboard.")
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("Dashboard server failed: %v", err)
	}
}

func (s *dashboardServer) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state, err := s.buildState()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *dashboardServer) handleTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state, err := s.buildState()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"activeTransfer": state.ActiveTransfer,
	})
}

func (s *dashboardServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req fetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ManifestCID == "" || req.TargetPeerID == "" {
		http.Error(w, "manifestCid and targetPeerId are required", http.StatusBadRequest)
		return
	}

	cfg, ok := s.peerConfigByID(req.TargetPeerID)
	if !ok {
		http.Error(w, "unknown target peer", http.StatusBadRequest)
		return
	}

	client, err := DialClient(cfg.Socket)
	if err != nil {
		http.Error(w, fmt.Sprintf("target peer unavailable: %v", err), http.StatusBadGateway)
		return
	}

	record := &dashboardFetchRecord{
		ManifestCID: req.ManifestCID,
		TargetPeer:  req.TargetPeerID,
		StartedAt:   time.Now(),
	}
	s.mu.Lock()
	s.fetches[req.TargetPeerID] = record
	s.mu.Unlock()

	go func() {
		defer client.rpcClient.Close()
		_, fetchErr := client.Fetch(req.ManifestCID)

		s.mu.Lock()
		defer s.mu.Unlock()
		record.FinishedAt = time.Now()
		if fetchErr != nil {
			record.LastError = fetchErr.Error()
		}
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}

func (s *dashboardServer) handlePeerRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/peers/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}

	peerID := parts[0]
	action := parts[1]
	cfg, ok := s.peerConfigByID(peerID)
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch action {
	case "log":
		s.handlePeerLog(w, r, cfg)
	default:
		http.NotFound(w, r)
	}
}

func (s *dashboardServer) handlePeerLog(w http.ResponseWriter, r *http.Request, cfg dashboardPeerConfig) {
	lines := 200
	if value := r.URL.Query().Get("lines"); value != "" {
		fmt.Sscanf(value, "%d", &lines)
	}
	text, err := tailFile(cfg.LogPath, lines)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, text)
}

func (s *dashboardServer) buildState() (dashboardStateResponse, error) {
	runtimes := make([]peerRuntimeState, 0, len(s.peers))
	peerIDToName := make(map[string]string)

	for _, cfg := range s.peers {
		runtime := peerRuntimeState{cfg: cfg, peerName: cfg.Name}
		client, err := DialClient(cfg.Socket)
		if err != nil {
			runtime.err = err
			runtimes = append(runtimes, runtime)
			continue
		}
		reply, err := client.State()
		client.rpcClient.Close()
		if err != nil {
			runtime.err = err
			runtimes = append(runtimes, runtime)
			continue
		}
		runtime.reply = reply
		runtime.peerID = reply.PeerID
		peerIDToName[reply.PeerID] = cfg.Name
		runtimes = append(runtimes, runtime)
	}

	filesByManifest := make(map[string]*dashboardFile)
	peers := make([]dashboardPeer, 0, len(runtimes))
	summary := dashboardSummary{}

	for _, runtime := range runtimes {
		peer := dashboardPeer{
			ID:        runtime.cfg.ID,
			Name:      runtime.cfg.Name,
			Online:    runtime.err == nil,
			Status:    offlineStatus(runtime.err),
			Socket:    runtime.cfg.Socket,
			ExportDir: runtime.cfg.ExportDir,
			LogPath:   runtime.cfg.LogPath,
		}

		if runtime.err == nil {
			summary.PeersOnline++
			peer.Stats = dashboardPeerStats{
				Files:           len(runtime.reply.Files),
				Pieces:          runtime.reply.AvailablePieceCount,
				ActiveTransfers: len(runtime.reply.Downloads),
			}
			peer.Files = make([]dashboardPeerFile, 0, len(runtime.reply.Files))
			for _, file := range runtime.reply.Files {
				peer.Files = append(peer.Files, dashboardPeerFile{
					Filename:    file.Filename,
					ManifestCID: file.ManifestCID,
					Size:        file.Size,
					PieceCount:  file.PieceCount,
				})
				entry := filesByManifest[file.ManifestCID]
				if entry == nil {
					entry = &dashboardFile{
						Filename:    file.Filename,
						ManifestCID: file.ManifestCID,
						Size:        file.Size,
						PieceCount:  file.PieceCount,
					}
					filesByManifest[file.ManifestCID] = entry
				}
				entry.Providers = append(entry.Providers, runtime.cfg.Name)
			}
		}

		peers = append(peers, peer)
	}

	files := make([]dashboardFile, 0, len(filesByManifest))
	for _, file := range filesByManifest {
		sort.Strings(file.Providers)
		file.DefaultTargetPeerID = s.chooseDefaultTargetPeer(*file, peers)
		files = append(files, *file)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Filename != files[j].Filename {
			return files[i].Filename < files[j].Filename
		}
		return files[i].ManifestCID < files[j].ManifestCID
	})
	summary.SeededFiles = len(files)

	state := dashboardStateResponse{
		Summary: summary,
		Config: dashboardConfig{
			IdlePollMs:     currentLocalDemoConfig().DashboardIdlePollMs,
			ActivePollMs:   currentLocalDemoConfig().DashboardActivePollMs,
			TransferPollMs: currentLocalDemoConfig().DashboardTransferPollMs,
		},
		Peers:   peers,
		Files:   files,
	}
	state.ActiveTransfer = s.buildActiveTransfer(runtimes, filesByManifest, peerIDToName)

	return state, nil
}

func (s *dashboardServer) buildActiveTransfer(runtimes []peerRuntimeState, filesByManifest map[string]*dashboardFile, peerIDToName map[string]string) *dashboardTransfer {
	var activeRuntime *peerRuntimeState
	var activeDownload *DownloadProgress

	for i := range runtimes {
		runtime := &runtimes[i]
		if runtime.err != nil || len(runtime.reply.Downloads) == 0 {
			continue
		}
		activeRuntime = runtime
		activeDownload = &runtime.reply.Downloads[0]
		break
	}

	if activeRuntime == nil || activeDownload == nil {
		s.cleanupFinishedFetches()
		return nil
	}

	catalog := filesByManifest[activeDownload.ManifestCID]
	providersEngaged := 0
	if catalog != nil {
		providersEngaged = len(catalog.Providers)
	}

	record := s.fetchRecord(activeRuntime.cfg.ID)
	elapsed := time.Duration(0)
	if record != nil {
		elapsed = time.Since(record.StartedAt)
	}

	var downloadedBytes int64
	pieces := make([]dashboardTransferPiece, 0, len(activeDownload.Pieces))
	for _, piece := range activeDownload.Pieces {
		state := "idle"
		owner := "Pending"
		if piece.Retrieved {
			state = "done"
			downloadedBytes += piece.Size
			owner = prettyPeerName(piece.SourcePeer, peerIDToName)
		} else if piece.InFlight {
			state = "active"
			owner = "In progress"
		}
		pieces = append(pieces, dashboardTransferPiece{
			Index: piece.Index,
			State: state,
			Owner: owner,
		})
	}

	return &dashboardTransfer{
		ManifestCID:      activeDownload.ManifestCID,
		Filename:         activeDownload.Filename,
		Downloader:       activeRuntime.cfg.Name,
		CompletedPieces:  activeDownload.CompletedPieces,
		TotalPieces:      activeDownload.PieceCount,
		ProvidersEngaged: providersEngaged,
		Throughput:       formatThroughput(downloadedBytes, elapsed),
		Pieces:           pieces,
	}
}

func (s *dashboardServer) chooseDefaultTargetPeer(file dashboardFile, peers []dashboardPeer) string {
	providers := make(map[string]struct{}, len(file.Providers))
	for _, name := range file.Providers {
		providers[name] = struct{}{}
	}
	for _, peer := range peers {
		if !peer.Online {
			continue
		}
		if _, isProvider := providers[peer.Name]; isProvider {
			continue
		}
		return peer.ID
	}
	for _, peer := range peers {
		if peer.Online {
			return peer.ID
		}
	}
	return ""
}

func (s *dashboardServer) fetchRecord(targetPeerID string) *dashboardFetchRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetches[targetPeerID]
}

func (s *dashboardServer) cleanupFinishedFetches() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for peerID, record := range s.fetches {
		if record == nil || record.FinishedAt.IsZero() {
			continue
		}
		if time.Since(record.FinishedAt) > 5*time.Second {
			delete(s.fetches, peerID)
		}
	}
}

func (s *dashboardServer) peerConfigByID(id string) (dashboardPeerConfig, bool) {
	for _, cfg := range s.peers {
		if cfg.ID == id {
			return cfg, true
		}
	}
	return dashboardPeerConfig{}, false
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func tailFile(path string, lines int) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	parts := strings.Split(text, "\n")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if lines > 0 && len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n"), nil
}

func prettyPeerName(source string, peerIDToName map[string]string) string {
	if source == "" {
		return "Retrieved"
	}
	if name, ok := peerIDToName[source]; ok {
		return name
	}
	return source
}

func formatThroughput(bytes int64, elapsed time.Duration) string {
	if bytes <= 0 || elapsed <= 0 {
		return "--"
	}
	bytesPerSecond := float64(bytes) / elapsed.Seconds()
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s"}
	value := bytesPerSecond
	unit := units[0]
	for _, nextUnit := range units[1:] {
		if value < 1024 {
			break
		}
		value /= 1024
		unit = nextUnit
	}
	if value >= 100 {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

func offlineStatus(err error) string {
	if err == nil {
		return "Online"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "Offline"
	}
	return "Offline"
}
