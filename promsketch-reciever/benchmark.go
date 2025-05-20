package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type Distribution struct {
	name string
	gen  func() float64
}

func generateUniform() float64 {
	return rand.Float64() * 1000
}

func generateNormal() float64 {
	return rand.NormFloat64()*100 + 500 // mean=500, stddev=100
}

func generateExponential() float64 {
	return rand.ExpFloat64() * 100
}

type MetricValue struct {
	Value float64 `json:"value"`
	Time  int64   `json:"time"`
}

func sendMetric(name string, value float64) error {
	// Send metric to test exporter via HTTP
	url := fmt.Sprintf("http://localhost:9101/metrics/%s", name)

	metric := MetricValue{
		Value: value,
		Time:  time.Now().UnixNano(),
	}

	jsonData, err := json.Marshal(metric)
	if err != nil {
		return err
	}

	_, err = http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	return err
}

type BenchmarkQuantileResponse struct {
	Values []float64 `json:"values"`
}

func queryQuantiles(name string, quantiles []float64) ([]float64, error) {
	// Query quantiles from receiver via HTTP
	url := fmt.Sprintf("http://localhost:8080/query/%s", name)

	jsonData, err := json.Marshal(quantiles)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result BenchmarkQuantileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Values, nil
}

func runBenchmark(dist Distribution, size int) (float64, float64) {
	// Initialize metrics
	metricName := fmt.Sprintf("test_metric_%s", dist.name)

	// Measure ingestion time
	start := time.Now()
	for i := 0; i < size; i++ {
		value := dist.gen()
		if err := sendMetric(metricName, value); err != nil {
			fmt.Printf("Error sending metric: %v\n", err)
			return 0, 0
		}
	}
	ingestionTime := time.Since(start).Seconds()

	// Wait for data to be processed
	time.Sleep(2 * time.Second)

	// Measure query time
	start = time.Now()
	quantiles := []float64{0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0}
	for i := 0; i < 1000; i++ { // Run 1000 queries to get average
		if _, err := queryQuantiles(metricName, quantiles); err != nil {
			fmt.Printf("Error querying quantiles: %v\n", err)
			return 0, 0
		}
	}
	queryTime := time.Since(start).Seconds() / 1000 // Average time per query

	return ingestionTime, queryTime
}

func RunBenchmark() {
	distributions := []Distribution{
		{"uniform", generateUniform},
		{"normal", generateNormal},
		{"exponential", generateExponential},
	}

	sizes := []int{1000, 10000, 100000, 1000000, 10000000}

	fmt.Println("Distribution,Size,IngestionTime,QueryTime")
	for _, dist := range distributions {
		for _, size := range sizes {
			ingestionTime, queryTime := runBenchmark(dist, size)
			fmt.Printf("%s,%d,%.6f,%.6f\n", dist.name, size, ingestionTime, queryTime)
		}
	}
}
