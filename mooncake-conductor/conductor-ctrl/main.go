package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"conductor/common"
	"conductor/kvevent"
)

var (
	// TODO change default config path
	conductorConfigPath = common.LoadEnv("CONDUCTOR_CONFIG_PATH", "/root/conductor_config.json")
	httpServerPort      = 13333
)

type configStruct struct {
	KVEventInstance map[string]serviceRaw `json:"kvevent_instance"`
	HTTPPort        int                   `json:"http_server_port"`
}

type serviceRaw struct {
	Endpoint       string `json:"endpoint"`
	ReplayEndpoint string `json:"replay_endpoint"`
	TypeStr        string `json:"type"`
	ModelName      string `json:"modelname"`
	LoraName       string `json:"lora_name"`
	TenantID       string `json:"tenant_id"`
	InstanceID     string `json:"instance_id"`
	BlockSize      int64  `json:"block_size"`
	DPRank         int    `json:"dp_rank"`
	AdditionalSalt string `json:"additionalsalt"`
}

func mapServiceType(s string) (string, bool) {
	switch s {
	case "vLLM":
		return common.ServiceTypeVLLM, true
	case "Mooncake":
		return common.ServiceTypeMooncake, true
	default:
		return "None", false
	}
}

func parseConfig() []common.ServiceConfig {
	if _, err := os.Stat(conductorConfigPath); errors.Is(err, os.ErrNotExist) {
		slog.Warn("Config file does not exist, exiting.", "path", conductorConfigPath)
		// os.Exit(1)
		return []common.ServiceConfig{}
	} else if err != nil {
		slog.Warn("Error accessing config file", "path", conductorConfigPath, "error", err)
		// os.Exit(1)
		return []common.ServiceConfig{}
	}

	data, err := os.ReadFile(conductorConfigPath)
	if err != nil {
		slog.Error("Failed to read config file", "path", conductorConfigPath, "error", err)
		os.Exit(1)
	}

	var cfg configStruct
	if err := json.Unmarshal(data, &cfg); err != nil {
		slog.Error("Failed to parse JSON config", "error", err)
		os.Exit(1)
	}
	httpServerPort = cfg.HTTPPort

	services := make([]common.ServiceConfig, 0, len(cfg.KVEventInstance))

	for name, raw := range cfg.KVEventInstance {
		serviceType, ok := mapServiceType(raw.TypeStr)
		if !ok {
			slog.Error("Unknown service type", "type", raw.TypeStr)
			continue
		}

		services = append(services, common.ServiceConfig{
			Endpoint:       raw.Endpoint,
			ReplayEndpoint: raw.ReplayEndpoint,
			Type:           serviceType,
			ModelName:      raw.ModelName,
			LoraName:       raw.LoraName,
			TenantID:       raw.TenantID,
			InstanceID:     name,
			BlockSize:      raw.BlockSize,
			DPRank:         raw.DPRank,
			AdditionalSalt: raw.AdditionalSalt,
		})
	}

	return services
}

func main() {
	// TODO support print metrics for conductor
	logLevel := common.ParseLogLevel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting Conductor KV Event Manager...", "logLevel", logLevel)

	services := parseConfig()

	manager := kvevent.NewEventManager(services, httpServerPort)

	if err := manager.StartHTTPServer(); err != nil {
		slog.Error("Failed to start HTTP server", "err", err)
	}

	if err := manager.Start(); err != nil {
		slog.Error("Failed to start manager", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Manager is running. Press Ctrl+C to stop.")
	<-sigChan

	slog.Info("Shutting down...")
	manager.Stop()
}
