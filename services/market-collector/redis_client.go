package main

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"time"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(addr string) (*Redis, error) {
	opts := &redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	}
	c := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Redis{client: c}, nil
}

func (r *Redis) Close() {
	_ = r.client.Close()
}

func (r *Redis) SetLatest(ctx context.Context, symbol string, t MarketTick) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	key := "latest:" + symbol
	return r.client.Set(ctx, key, b, 0).Err()
}

func (r *Redis) GetLatest(ctx context.Context, symbol string) (*MarketTick, error) {
	key := "latest:" + symbol
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	var t MarketTick
	if err := json.Unmarshal(val, &t); err != nil {
		return nil, err
	}
	return &t, nil
}