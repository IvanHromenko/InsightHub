package main

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	conn *sqlx.DB
}

func NewDB(pgURL string) (*DB, error) {
	db, err := sqlx.Connect("postgres", pgURL)
	if err != nil {
		return nil, err
	}
	return &DB{conn: db}, nil
}

func (d *DB) Close() {
	_ = d.conn.Close()
}

func (d *DB) InsertMarketTick(ctx context.Context, t *MarketTick) error {
	query := `
INSERT INTO market_data (asset_symbol, timestamp, open, high, low, close, volume, source, raw_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`
	_, err := d.conn.ExecContext(ctx, query,
		t.AssetSymbol, t.Timestamp, t.Open, t.High, t.Low, t.Close, t.Volume, t.Source, t.RawJSON)
	if err != nil {
		return fmt.Errorf("insert market tick: %w", err)
	}
	return nil
}