package main

import "time"

type MarketTick struct {
	AssetSymbol string                 `json:"asset_symbol"`
	Timestamp   time.Time              `json:"timestamp"`
	Open        float64                `json:"open"`
	High        float64                `json:"high"`
	Low         float64                `json:"low"`
	Close       float64                `json:"close"`
	Volume      float64                `json:"volume"`
	Source      string                 `json:"source"`
	RawJSON     map[string]interface{} `json:"raw_json"`
}