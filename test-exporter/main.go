package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var (
	startTime = time.Now()
	endTime   = startTime.Add(3 * time.Minute)
)

func main() {
	// Start metrics generation in a goroutine
	go generateMetrics()

	// Set up HTTP server for metrics endpoint
	http.HandleFunc("/metrics", metricsHandler)

	log.Printf("Test exporter starting on :9101 (will run for 3 minutes)")
	if err := http.ListenAndServe(":9101", nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

func generateMetrics() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Now().After(endTime) {
				log.Printf("3 minutes elapsed, shutting down...")
				return
			}
			// Generate new metrics
			updateMetrics()
		}
	}
}

var (
	normalValue      = 500.0
	exponentialValue = 200.0
	uniformValue     = 500.0
)

func updateMetrics() {
	// Update values with some random variation
	normalValue = 500 + rand.NormFloat64()*50
	exponentialValue = 200 * rand.ExpFloat64()
	uniformValue = rand.Float64() * 1000
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	// Check if we should still be running
	if time.Now().After(endTime) {
		http.Error(w, "Exporter has completed its 3-minute run", http.StatusServiceUnavailable)
		return
	}

	// Output metrics in Prometheus format
	fmt.Fprintf(w, "# HELP test_normal A normally distributed test metric\n")
	fmt.Fprintf(w, "# TYPE test_normal gauge\n")
	fmt.Fprintf(w, "test_normal %f\n", normalValue)

	fmt.Fprintf(w, "# HELP test_exponential An exponentially distributed test metric\n")
	fmt.Fprintf(w, "# TYPE test_exponential gauge\n")
	fmt.Fprintf(w, "test_exponential %f\n", exponentialValue)

	fmt.Fprintf(w, "# HELP test_uniform A uniformly distributed test metric\n")
	fmt.Fprintf(w, "# TYPE test_uniform gauge\n")
	fmt.Fprintf(w, "test_uniform %f\n", uniformValue)

	// Add a metric to show time remaining
	timeRemaining := endTime.Sub(time.Now()).Seconds()
	fmt.Fprintf(w, "# HELP test_time_remaining Seconds remaining in the test run\n")
	fmt.Fprintf(w, "# TYPE test_time_remaining gauge\n")
	fmt.Fprintf(w, "test_time_remaining %f\n", timeRemaining)
}
