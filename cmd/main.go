// Command gomqtt runs the MQTT 3.1.1 broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/anashasan/gomqtt/cmd/server"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/di"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
)

// Build metadata, stamped by the linker. See the Makefile's LDFLAGS.
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String("config", os.Getenv("GOMQTT_CONFIG_PATH"), "path to the YAML config file")
	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gomqtt %s (commit %s, built %s)\n", version, commit, buildTime)
		return
	}

	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// run owns the whole process lifecycle and returns an error rather than calling
// os.Exit, so every deferred cleanup below actually runs.
func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if version != "dev" {
		cfg.Broker.Version = version
	}

	log := di.ProvideLogger(cfg)

	// Signals are trapped before anything is started, so a Ctrl-C during
	// initialisation is still handled gracefully rather than killing the
	// process half-built.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info(ctx, "starting gomqtt",
		logger.F("version", cfg.Broker.Version),
		logger.F("commit", commit),
		logger.F("env", cfg.Env),
		logger.F("mqtt_address", cfg.Broker.Address),
		logger.F("max_connections", cfg.Broker.MaxConnections),
		logger.F("allow_anonymous", cfg.Auth.AllowAnonymous),
	)

	registry := di.ProvideMetricsRegistry()
	recorder := di.ProvideMetricsRecorder(registry)
	broker := di.InjectBroker(cfg, recorder)

	srv := server.NewServer(server.Options{
		Config:     cfg,
		Listener:   broker.Listener,
		Handlers:   broker.Handlers,
		Publishing: broker.Publishing,
		Sessions:   broker.Sessions,
		Index:      broker.Index,
		Registry:   registry,
		Metrics:    recorder,
		IDs:        di.ProvideUIDGenerator(),
		Log:        log,
	})

	if err := srv.Start(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	log.Info(ctx, "shutdown signal received")

	// A fresh context: the signal context is already cancelled, and passing it
	// to Stop would make every shutdown step return immediately without
	// draining.
	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return srv.Stop(shutdownCtx)
}
