package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	cache map[time.Time]float64
	mut   sync.Mutex
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx); !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)

	cache, err = fetchPrices(
		ctx,
		time.Date(2018, time.October, 1, 0, 0, 0, 0, time.UTC),
		time.Now(),
	)

	if err != nil {
		return fmt.Errorf("error fetching prices: %w", err)
	}

	go func() {
		ticker := time.NewTicker(6 * time.Hour)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prices, err := fetchPrices(ctx, time.Now(), time.Now().Add(-7*time.Hour))
				if err != nil {
					cancel(fmt.Errorf("error fetching prices: %w", err))
				}

				mut.Lock()
				for t, p := range prices {
					cache[t] = p
				}
				mut.Unlock()
			}
		}
	}()

	s := http.Server{
		Addr:    net.JoinHostPort("", "2002"),
		Handler: http.HandlerFunc(handler),
	}
	log.Printf("serving on %s\n", s.Addr)

	go func() {
		<-ctx.Done()
		s.Shutdown(context.Background())
	}()

	if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error listening on %s: %w", s.Addr, err)
	}

	return context.Cause(ctx)
}

func handler(w http.ResponseWriter, _ *http.Request) {
	var response []any
	for t, p := range cache {
		response = append(response, struct {
			T int64   `json:"time"`
			P float64 `json:"price"`
		}{t.Unix(), p})
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(bytes)
}

func fetchPrices(
	ctx context.Context,
	start time.Time,
	end time.Time,
) (map[time.Time]float64, error) {
	q := url.Values{}
	if !start.IsZero() {
		q.Set("start", start.Format(time.RFC3339))
	}
	if !end.IsZero() {
		q.Set("end", end.Format(time.RFC3339))
	}

	// The data is licensed as CC BY 4.0 from Bundesnetzagentur | SMARD.de
	u := url.URL{
		Scheme:   "https",
		Host:     "api.energy-charts.info",
		Path:     "/price",
		RawQuery: q.Encode(),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching prices: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response status: %s", res.Status)
	}

	var payload marketPrices
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("error parsing repsponse body: %w", err)
	}

	if payload.Unit != "EUR/MWh" {
		return nil, fmt.Errorf("unexpected unit: %s", payload.Unit)
	}

	if payload.Deprecated {
		return nil, fmt.Errorf("api for %s is marked deprecated", u.String())
	}

	if len(payload.Timestamps) != len(payload.Prices) {
		return nil, fmt.Errorf(
			"expected equal number of timestamps and prices in response, got %d and %d",
			len(payload.Timestamps), len(payload.Prices),
		)
	}

	prices := make(map[time.Time]float64)
	for i, t := range payload.Timestamps {
		prices[time.Unix(t, 0)] = payload.Prices[i]
	}

	return prices, nil
}

type marketPrices struct {
	Timestamps []int64   `json:"unix_seconds"`
	Prices     []float64 `json:"price"`
	Unit       string
	Deprecated bool
}
