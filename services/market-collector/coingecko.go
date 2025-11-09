package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

func FetchCoinGeckoSimplePrice(ctx context.Context, coinID string) (price float64, volume float64, err error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd&include_24hr_vol=true", coinID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, 0, fmt.Errorf("non-200 from coingecko: %d", resp.StatusCode)
	}

	var result map[string]map[string]json.RawMessage
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&result); err != nil {
		return 0, 0, err
	}

	item, ok := result[coinID]
	if !ok {
		return 0, 0, fmt.Errorf("missing coin %s in response", coinID)
	}

	// decode usd price
	var p float64
	if raw, ok := item["usd"]; ok {
		if err := json.Unmarshal(raw, &p); err == nil {
			price = p
		}
	}
	// decode 24h vol
	var v float64
	if raw, ok := item["usd_24h_vol"]; ok {
		if err := json.Unmarshal(raw, &v); err == nil {
			volume = v
		}
	}
	return price, volume, nil
}