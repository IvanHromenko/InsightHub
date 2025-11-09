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
	RabbitURL    string
	PollInterval time.Duration
}

type App struct {
	cfg *AppConfig
	db  *DB     // wrapper around sqlx
	rdb *Redis  // wrapper around go-redis
	rp  *RabbitP // wrapper around rabbitmq publisher
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
	rp, err := NewRabbitP(cfg.RabbitURL, "market.ticks")
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, db: db, rdb: rdb, rp: rp}, nil
}

func (a *App) Close() {
	if a.db != nil {
		a.db.Close()
	}
	if a.rdb != nil {
		a.rdb.Close()
	}
	if a.rp != nil {
		a.rp.Close()
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
			Open:        price,
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

		// publish to RabbitMQ
		msgBytes, _ := json.Marshal(tick)
		if err := a.rp.Publish(ctx, msgBytes); err != nil {
			log.Printf("rabbit publish failed for %s: %v", asset, err)
		}
	}
	return nil
}