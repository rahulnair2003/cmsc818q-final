// Revised promsketch ingestion server with accurate memory measurement
package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// MetricProcessor handles processing of metrics using DDSketch
type MetricProcessor struct {
	sketches map[string]*DDSketch
	mu       sync.RWMutex
}

// NewMetricProcessor creates a new MetricProcessor
func NewMetricProcessor() *MetricProcessor {
	return &MetricProcessor{
		sketches: make(map[string]*DDSketch),
	}
}

// ProcessMetrics processes incoming Prometheus metrics
func (p *MetricProcessor) ProcessMetrics(wr *prompb.WriteRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, ts := range wr.Timeseries {
		// Create metric name from labels
		metricName := ""
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				metricName = label.Value
				break
			}
		}
		if metricName == "" {
			continue
		}

		// Get or create sketch for this metric
		sketch, exists := p.sketches[metricName]
		if !exists {
			var err error
			sketch, err = NewDDSketch(0.01) // 1% relative accuracy
			if err != nil {
				return err
			}
			p.sketches[metricName] = sketch
		}

		// Process samples
		for _, sample := range ts.Samples {
			sketch.Add(sample.Value)
		}
	}
	return nil
}

// GetSketch returns the sketch for a given metric name
func (p *MetricProcessor) GetSketch(metricName string) *DDSketch {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.sketches[metricName]
}

// GenerateTestData generates test data with different distributions
func (p *MetricProcessor) GenerateTestData() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create sketches for different distributions
	distributions := map[string]func() float64{
		"uniform": func() float64 { return rand.Float64() * 1000 },
		"normal": func() float64 {
			return rand.NormFloat64()*100 + 500 // mean=500, std=100
		},
		"exponential": func() float64 {
			return rand.ExpFloat64() * 200 // scale=200
		},
		"sequential": func() float64 {
			return float64(rand.Intn(1000))
		},
	}

	// Generate data for each distribution
	for name, dist := range distributions {
		sketch, err := NewDDSketch(0.01)
		if err != nil {
			return err
		}

		// Generate 1000000 values
		for i := 0; i < 10000000; i++ {
			sketch.Add(dist())
		}

		p.sketches["test_"+name] = sketch
	}

	return nil
}

// MergeTestSketches merges all test sketches into a single result
func (p *MetricProcessor) MergeTestSketches() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create a new sketch for merged results
	merged, err := NewDDSketch(0.01)
	if err != nil {
		return err
	}

	// Merge all test sketches
	for name, sketch := range p.sketches {
		if name[:5] == "test_" {
			// Add all values from the sketch
			for i := int64(0); i < sketch.GetCount(); i++ {
				merged.Add(sketch.Quantile(float64(i) / float64(sketch.GetCount())))
			}
		}
	}

	p.sketches["test_merged"] = merged
	return nil
}

// SpeedTestResponse represents the results of a speed test
type SpeedTestResponse struct {
	Metric            string  `json:"metric"`
	IngestionTimeMs   float64 `json:"ingestion_time_ms"`
	IngestionRate     float64 `json:"ingestion_rate"` // values per second
	QueryTimeMs       float64 `json:"query_time_ms"`
	QueryRate         float64 `json:"query_rate"` // queries per second
	ValuesProcessed   int64   `json:"values_processed"`
	QueriesProcessed  int     `json:"queries_processed"`
	AllocationTimeMs  float64 `json:"allocation_time_ms"`
	PerQuantileTimeMs float64 `json:"per_quantile_time_ms"`
	TotalTimeMs       float64 `json:"total_time_ms"`
	// Raw array comparison
	RawArrayIngestionTimeMs float64 `json:"raw_array_ingestion_time_ms"`
	RawArrayQueryTimeMs     float64 `json:"raw_array_query_time_ms"`
	RawArrayMemoryBytes     int64   `json:"raw_array_memory_bytes"`
	SketchMemoryBytes       int64   `json:"sketch_memory_bytes"`
	MemorySavingsPercent    float64 `json:"memory_savings_percent"`
}

func (p *MetricProcessor) speedTestHandler(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		metric = "test_uniform"
	}

	// Create a new sketch for testing
	sketch, err := NewDDSketch(0.01)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Generate test values based on distribution
	numValues := 100000 // 100K values instead of 1M
	startAlloc := time.Now()
	values := make([]float64, numValues)

	// Select distribution based on metric name
	switch metric {
	case "test_uniform":
		for i := range values {
			values[i] = rand.Float64() * 1000
		}
	case "test_normal":
		for i := range values {
			values[i] = rand.NormFloat64()*100 + 500 // mean=500, std=100
		}
	case "test_exponential":
		for i := range values {
			values[i] = rand.ExpFloat64() * 200 // scale=200
		}
	case "test_sequential":
		for i := range values {
			values[i] = float64(rand.Intn(1000))
		}
	default:
		http.Error(w, "Invalid distribution type. Use: test_uniform, test_normal, test_exponential, or test_sequential", http.StatusBadRequest)
		return
	}
	allocTime := time.Since(startAlloc)

	// Warm-up phase
	for i := 0; i < 1000; i++ {
		sketch.Add(values[i%len(values)])
	}
	runtime.GC()

	// Measure sketch ingestion time
	startIngest := time.Now()
	for _, v := range values {
		sketch.Add(v)
	}
	ingestTime := time.Since(startIngest)

	// Measure sketch query time
	numQueries := 1000
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}
	startQuery := time.Now()
	for i := 0; i < numQueries; i++ {
		for _, q := range quantiles {
			sketch.Quantile(q)
		}
	}
	queryTime := time.Since(startQuery)

	// Measure raw array performance
	startRawIngest := time.Now()
	rawArray := make([]float64, len(values))
	copy(rawArray, values)
	rawIngestTime := time.Since(startRawIngest)

	// Measure raw array query time (sorting + quantile calculation)
	startRawQuery := time.Now()
	for i := 0; i < numQueries; i++ {
		// Create a copy for sorting
		sorted := make([]float64, len(rawArray))
		copy(sorted, rawArray)
		sort.Float64s(sorted)

		// Calculate quantiles
		for _, q := range quantiles {
			idx := int(float64(len(sorted)-1) * q)
			_ = sorted[idx] // Get quantile value
		}
	}
	rawQueryTime := time.Since(startRawQuery)

	// Calculate memory usage
	rawArrayMemory := int64(len(rawArray) * 8) // 8 bytes per float64

	// More accurate DDSketch memory measurement
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	sk, _ := NewDDSketch(0.01)
	for _, v := range values {
		sk.Add(v)
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	sketchMemory := int64(m2.HeapAlloc - m1.HeapAlloc)

	// Calculate rates and per-operation times
	ingestRate := float64(numValues) / ingestTime.Seconds()
	queryRate := float64(numQueries) / queryTime.Seconds()
	perQuantileTime := queryTime.Seconds() / float64(numQueries*len(quantiles)) * 1000 // ms per quantile
	totalTime := allocTime.Seconds() + ingestTime.Seconds() + queryTime.Seconds()

	// Calculate memory savings
	memorySavings := 0.0
	if rawArrayMemory > 0 {
		memorySavings = (1 - float64(sketchMemory)/float64(rawArrayMemory)) * 100
	}

	// Return results
	response := SpeedTestResponse{
		Metric:                  metric,
		IngestionTimeMs:         ingestTime.Seconds() * 1000,
		IngestionRate:           ingestRate,
		QueryTimeMs:             queryTime.Seconds() * 1000,
		QueryRate:               queryRate,
		ValuesProcessed:         int64(numValues),
		QueriesProcessed:        numQueries,
		AllocationTimeMs:        allocTime.Seconds() * 1000,
		PerQuantileTimeMs:       perQuantileTime,
		TotalTimeMs:             totalTime * 1000,
		RawArrayIngestionTimeMs: rawIngestTime.Seconds() * 1000,
		RawArrayQueryTimeMs:     rawQueryTime.Seconds() * 1000,
		RawArrayMemoryBytes:     rawArrayMemory,
		SketchMemoryBytes:       sketchMemory,
		MemorySavingsPercent:    memorySavings,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// QuantileResponse represents the response for a quantile query
type QuantileResponse struct {
	Metric   string  `json:"metric"`
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
	Count    int64   `json:"count"`
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Sum      float64 `json:"sum"`
}

func (p *MetricProcessor) quantileHandler(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	quantileStr := r.URL.Query().Get("q")
	if quantileStr == "" {
		http.Error(w, "q parameter (quantile) is required", http.StatusBadRequest)
		return
	}

	quantile, err := strconv.ParseFloat(quantileStr, 64)
	if err != nil || quantile < 0 || quantile > 1 {
		http.Error(w, "quantile must be a number between 0 and 1", http.StatusBadRequest)
		return
	}

	sketch := p.GetSketch(metric)
	if sketch == nil {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}

	response := QuantileResponse{
		Metric:   metric,
		Quantile: quantile,
		Value:    sketch.Quantile(quantile),
		Count:    sketch.GetCount(),
		Min:      sketch.GetMin(),
		Max:      sketch.GetMax(),
		Sum:      sketch.GetSum(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	processor := NewMetricProcessor()
	mux := http.NewServeMux()

	// Handle Prometheus remote_write endpoint
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := processor.ProcessMetrics(&req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	// Add quantile endpoint
	mux.HandleFunc("/quantile", processor.quantileHandler)

	// Add speed test endpoint
	mux.HandleFunc("/speedtest", processor.speedTestHandler)

	// Add test data generation endpoint
	mux.HandleFunc("/test/generate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := processor.GenerateTestData(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	// Create a server with timeouts
	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("Ingestion server running on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
