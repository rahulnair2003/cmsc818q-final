package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type TestExporter struct {
	uniform     []float64
	normal      []float64
	exponential []float64
	size        int
}

func NewTestExporter() *TestExporter {
	return &TestExporter{
		size: 1000, // default size
	}
}

func (e *TestExporter) generateMetrics() {
	// Resize arrays if needed
	if len(e.uniform) != e.size {
		e.uniform = make([]float64, e.size)
		e.normal = make([]float64, e.size)
		e.exponential = make([]float64, e.size)
	}

	// Generate values for each distribution
	for i := 0; i < e.size; i++ {
		// Uniform distribution (0-1000)
		e.uniform[i] = rand.Float64() * 1000

		// Normal distribution (mean=500, std=100)
		e.normal[i] = rand.NormFloat64()*100 + 500

		// Exponential distribution (scale=200)
		e.exponential[i] = rand.ExpFloat64() * 200
	}
}

func (e *TestExporter) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// Get size parameter from query
	if sizeStr := r.URL.Query().Get("size"); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil {
			e.size = size
		}
	}

	e.generateMetrics()

	// Output metrics in Prometheus format
	w.Header().Set("Content-Type", "text/plain")

	// Uniform distribution
	w.Write([]byte("# HELP test_uniform A metric with uniform distribution\n"))
	w.Write([]byte("# TYPE test_uniform gauge\n"))
	for i, v := range e.uniform {
		w.Write([]byte(fmt.Sprintf("test_uniform{index=\"%d\"} %f\n", i, v)))
	}
	w.Write([]byte("\n"))

	// Normal distribution
	w.Write([]byte("# HELP test_normal A metric with normal distribution\n"))
	w.Write([]byte("# TYPE test_normal gauge\n"))
	for i, v := range e.normal {
		w.Write([]byte(fmt.Sprintf("test_normal{index=\"%d\"} %f\n", i, v)))
	}
	w.Write([]byte("\n"))

	// Exponential distribution
	w.Write([]byte("# HELP test_exponential A metric with exponential distribution\n"))
	w.Write([]byte("# TYPE test_exponential gauge\n"))
	for i, v := range e.exponential {
		w.Write([]byte(fmt.Sprintf("test_exponential{index=\"%d\"} %f\n", i, v)))
	}
}

func main() {
	exporter := NewTestExporter()
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", exporter.metricsHandler)

	// Create a server with timeouts
	server := &http.Server{
		Addr:         ":9101",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start test exporter
	log.Println("Test exporter running on :9101")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("Test exporter error: %v", err)
	}
}
