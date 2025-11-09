package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type AppConfig struct {
	Assets       []string
	PGURL        string
	RedisAddr    string
	KafkaBroker  string
	PollInterval time.Duration
}

type App struct {
	cfg *AppConfig
	db  *DB     // wrapper around sqlx
	rdb *Redis  // wrapper around go-redis
	kp  *KafkaP // wrapper around kafka writer
}

func NewApp(cfg *AppConfig) (*App, error) {
	db, err := NewDB(cfg.PGURL)
	if err != nil {
		return nil, err
	}
	rdb, err := NewRedis(cfg.RedisAddr)
	if err != nil {
		return nil, err
	}
	kp, err := NewKafkaP(cfg.KafkaBroker, "market.ticks")
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, db: db, rdb: rdb, kp: kp}, nil
}

func (a *App) Close() {
	if a.db != nil {
		a.db.Close()
	}
	if a.rdb != nil {
		a.rdb.Close()
	}
	if a.kp != nil {
		a.kp.Close()
	}
}

// FetchAndPublish - fetch current prices for configured assets and persist/publish
func (a *App) FetchAndPublish(ctx context.Context) error {
	now := time.Now().UTC()
	for _, asset := range a.cfg.Assets {
		asset = trimSpace(asset)
		// fetch from CoinGecko
		price, vol, err := FetchCoinGeckoSimplePrice(ctx, asset)
		if err != nil {
			log.Printf("error fetching %s: %v", asset, err)
			continue
		}

		// normalize -> market tick struct
		tick := MarketTick{
			AssetSymbol: asset,
			Timestamp:   now,
			Open:        price, // CoinGecko provides current price only; for MVP we use same value
			High:        price,
			Low:         price,
			Close:       price,
			Volume:      vol,
			Source:      "coingecko",
			RawJSON:     map[string]interface{}{"source": "coingecko"},
		}

		// write to Postgres
		if err := a.db.InsertMarketTick(ctx, &tick); err != nil {
			log.Printf("failed to insert tick for %s: %v", asset, err)
		}

		// cache latest in Redis
		if err := a.rdb.SetLatest(ctx, tick.AssetSymbol, tick); err != nil {
			log.Printf("redis set failed for %s: %v", asset, err)
		}

		// publish to Kafka
		msgBytes, _ := json.Marshal(tick)
		if err := a.kp.Publish(ctx, msgBytes); err != nil {
			log.Printf("kafka publish failed for %s: %v", asset, err)
		} else {
			fmt.Printf("published tick for %s at %s\n", asset, now.Format(time.RFC3339))
		}
	}
	return nil
}