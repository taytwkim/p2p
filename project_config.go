package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type systemConfig struct {
	DefaultPieceSizeBytes          int64
	MaxDownloadWorkers             int
	PeerDownloadRateAlpha          float64
	ProviderQueryLimit             int
	ChokeReevaluationInterval      time.Duration
	OptimisticUnchokeRotationRounds int
	MaxUnchokedPeersPerManifest    int
	OptimisticUnchokeSlots         int
}

type localDemoConfig struct {
	SeedTextSourceRel       string
	SeedTextDestName        string
	DashboardUIRel          string
	DashboardIdlePollMs     int
	DashboardActivePollMs   int
	DashboardTransferPollMs int
}

var (
	systemConfigOnce   sync.Once
	systemConfigCache  systemConfig
	localDemoConfigOnce  sync.Once
	localDemoConfigCache localDemoConfig
)

func defaultSystemConfig() systemConfig {
	return systemConfig{
		DefaultPieceSizeBytes:           8 * 1024,
		MaxDownloadWorkers:              4,
		PeerDownloadRateAlpha:           0.8,
		ProviderQueryLimit:              20,
		ChokeReevaluationInterval:       10 * time.Second,
		OptimisticUnchokeRotationRounds: 3,
		MaxUnchokedPeersPerManifest:     4,
		OptimisticUnchokeSlots:          1,
	}
}

func defaultLocalDemoConfig() localDemoConfig {
	return localDemoConfig{
		SeedTextSourceRel:       "demo/local/alice.txt",
		SeedTextDestName:        "alice.txt",
		DashboardUIRel:          "demo/local/ui",
		DashboardIdlePollMs:     2000,
		DashboardActivePollMs:   500,
		DashboardTransferPollMs: 150,
	}
}

func currentSystemConfig() systemConfig {
	systemConfigOnce.Do(func() {
		cfg := defaultSystemConfig()
		cwd, err := os.Getwd()
		if err == nil {
			cfg = loadSystemConfig(cwd)
		}
		systemConfigCache = cfg
	})
	return systemConfigCache
}

func currentLocalDemoConfig() localDemoConfig {
	localDemoConfigOnce.Do(func() {
		cfg := defaultLocalDemoConfig()
		cwd, err := os.Getwd()
		if err == nil {
			cfg = loadLocalDemoConfig(cwd)
		}
		localDemoConfigCache = cfg
	})
	return localDemoConfigCache
}

func loadSystemConfig(repoRoot string) systemConfig {
	cfg := defaultSystemConfig()
	configPath := filepath.Join(repoRoot, "config.env")

	file, err := os.Open(configPath)
	if err != nil {
		return cfg
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "DEFAULT_PIECE_SIZE_BYTES":
			if parsed, err := strconv.ParseInt(value, 10, 64); err == nil && parsed > 0 {
				cfg.DefaultPieceSizeBytes = parsed
			}
		case "MAX_DOWNLOAD_WORKERS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.MaxDownloadWorkers = parsed
			}
		case "PEER_DOWNLOAD_RATE_ALPHA":
			if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed > 0 && parsed <= 1 {
				cfg.PeerDownloadRateAlpha = parsed
			}
		case "PROVIDER_QUERY_LIMIT":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.ProviderQueryLimit = parsed
			}
		case "CHOKE_REEVALUATION_INTERVAL_MS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.ChokeReevaluationInterval = time.Duration(parsed) * time.Millisecond
			}
		case "OPTIMISTIC_UNCHOKE_ROTATION_ROUNDS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.OptimisticUnchokeRotationRounds = parsed
			}
		case "MAX_UNCHOKED_PEERS_PER_MANIFEST":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.MaxUnchokedPeersPerManifest = parsed
			}
		case "OPTIMISTIC_UNCHOKE_SLOTS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed >= 0 {
				cfg.OptimisticUnchokeSlots = parsed
			}
		}
	}

	return cfg
}

func loadLocalDemoConfig(repoRoot string) localDemoConfig {
	cfg := defaultLocalDemoConfig()
	configPath := filepath.Join(repoRoot, "demo", "local", "config.env")

	file, err := os.Open(configPath)
	if err != nil {
		return cfg
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "SEED_TEXT_SOURCE_REL":
			if value != "" {
				cfg.SeedTextSourceRel = value
			}
		case "SEED_TEXT_DEST_NAME":
			if value != "" {
				cfg.SeedTextDestName = value
			}
		case "DASHBOARD_UI_REL":
			if value != "" {
				cfg.DashboardUIRel = value
			}
		case "DASHBOARD_IDLE_POLL_MS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.DashboardIdlePollMs = parsed
			}
		case "DASHBOARD_ACTIVE_POLL_MS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.DashboardActivePollMs = parsed
			}
		case "DASHBOARD_TRANSFER_POLL_MS":
			if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
				cfg.DashboardTransferPollMs = parsed
			}
		}
	}

	return cfg
}
