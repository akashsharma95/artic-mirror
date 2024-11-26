package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"arctic-mirror/config"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize components
	replicator, err := replication.NewReplicator(cfg)
	if err != nil {
		log.Fatalf("Failed to create replicator: %v", err)
	}

	proxy, err := proxy.NewDuckDBProxy(cfg)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start replication
	go func() {
		if err := replicator.Start(ctx); err != nil {
			log.Printf("Replication error: %v", err)
			cancel()
		}
	}()

	// Start proxy server
	go func() {
		if err := proxy.Start(ctx); err != nil {
			log.Printf("Proxy error: %v", err)
			cancel()
		}
	}()

	// Wait for shutdown signal
	select {
	case <-sigChan:
		log.Println("Shutting down...")
	case <-ctx.Done():
		log.Println("Context cancelled...")
	}
}
