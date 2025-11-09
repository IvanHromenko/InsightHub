package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	assetsEnv := getenv("COINGECKO_ASSETS", "bitcoin,ethereum")
	assets := strings.Split(assetsEnv, ",")

	pgURL := getenv("PG_URL", "postgres://insighthub:secret@localhost:5432/insighthub?sslmode=disable")
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	kafkaBroker := getenv("KAFKA_BROKER", "localhost:9092")
	pollIntervalSec := getenvInt("POLL_INTERVAL_SECONDS", 30)

	cfg := &AppConfig{
		Assets:        assets,
		PGURL:         pgURL,
		RedisAddr:     redisAddr,
		KafkaBroker:   kafkaBroker,
		PollInterval:  time.Duration(pollIntervalSec) * time.Second,
	}

	app, err := NewApp(cfg)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}
	defer app.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// poll loop
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// run one fetch immediately
	if err := app.FetchAndPublish(ctx); err != nil {
		log.Printf("initial fetch failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down collector")
			return
		case <-ticker.C:
			if err := app.FetchAndPublish(ctx); err != nil {
				log.Printf("fetch cycle failed: %v", err)
			}
		}
	}
}