package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// TimeWindowedSketch represents a sketch with a time window
type TimeWindowedSketch struct {
	sketch     *DDSketch
	windowSize time.Duration
	mu         sync.RWMutex
}

// NewTimeWindowedSketch creates a new time-windowed sketch
func NewTimeWindowedSketch(windowSize time.Duration) (*TimeWindowedSketch, error) {
	sketch, err := NewDDSketch(0.01)
	if err != nil {
		return nil, err
	}
	return &TimeWindowedSketch{
		sketch:     sketch,
		windowSize: windowSize,
	}, nil
}

// Add adds a value to the sketch with a timestamp
func (t *TimeWindowedSketch) Add(value float64, timestamp time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sketch.Add(value)
}

// GetSketch returns the current sketch
func (t *TimeWindowedSketch) GetSketch() *DDSketch {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sketch
}

// TestWindowedSketch handles testing of time-windowed sketches
type TestWindowedSketch struct {
	sketches map[string]*TimeWindowedSketch
	mu       sync.RWMutex
}

// NewTestWindowedSketch creates a new test windowed sketch
func NewTestWindowedSketch() *TestWindowedSketch {
	return &TestWindowedSketch{
		sketches: make(map[string]*TimeWindowedSketch),
	}
}

// GenerateTestData generates test data for a specific window
func (t *TestWindowedSketch) GenerateTestData(window string, count int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sketch, exists := t.sketches[window]
	if !exists {
		var err error
		sketch, err = NewTimeWindowedSketch(5 * time.Minute)
		if err != nil {
			return err
		}
		t.sketches[window] = sketch
	}

	// Generate values with timestamps
	now := time.Now()
	for i := 0; i < count; i++ {
		value := rand.Float64() * 1000
		timestamp := now.Add(time.Duration(i) * time.Second)
		sketch.Add(value, timestamp)
	}

	return nil
}

// GetSketch returns the sketch for a specific window
func (t *TestWindowedSketch) GetSketch(window string) *TimeWindowedSketch {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sketches[window]
}

// StartWindowTestServer starts a test server for windowed sketches
func StartWindowTestServer() {
	testSketch := NewTestWindowedSketch()

	// Handle test data generation for windows
	http.HandleFunc("/window/generate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		window := r.URL.Query().Get("window")
		if window == "" {
			window = "default"
		}

		count := 1000 // default
		if c := r.URL.Query().Get("count"); c != "" {
			if n, err := strconv.Atoi(c); err == nil {
				count = n
			}
		}

		if err := testSketch.GenerateTestData(window, count); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	// Handle window metrics query
	http.HandleFunc("/window/metrics", func(w http.ResponseWriter, r *http.Request) {
		window := r.URL.Query().Get("window")
		if window == "" {
			window = "default"
		}

		sketch := testSketch.GetSketch(window)
		if sketch == nil {
			http.Error(w, "window not found", http.StatusNotFound)
			return
		}

		ddsketch := sketch.GetSketch()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"window": window,
			"count":  ddsketch.GetCount(),
			"min":    ddsketch.GetMin(),
			"max":    ddsketch.GetMax(),
			"sum":    ddsketch.GetSum(),
			"p50":    ddsketch.Quantile(0.5),
			"p90":    ddsketch.Quantile(0.9),
			"p99":    ddsketch.Quantile(0.99),
		})
	})

	// Start the server
	log.Println("Window test server running on :9092")
	log.Fatal(http.ListenAndServe(":9092", nil))
}
