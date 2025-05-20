package promsketch

import (
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

type DistributionResult struct {
	Label       string    `json:"label"`
	Count       int64     `json:"count"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Sum         float64   `json:"sum"`
	Errors      []float64 `json:"errors"`
	MergeErrors []float64 `json:"merge_errors,omitempty"`
	InsertTime  float64   `json:"insert_time_ms"`
	MergeTime   float64   `json:"merge_time_ms,omitempty"`
	QueryTime   float64   `json:"query_time_ms,omitempty"`
}

func TestDDSketchProfiling(t *testing.T) {
	const numValues = 10000000
	relAccuracy := 0.01
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}

	distributions := []struct {
		label        string
		distribution func(i int) float64
	}{
		{
			label: "Uniform",
			distribution: func(_ int) float64 {
				return rand.Float64() * 1000
			},
		},
		{
			label: "Normal",
			distribution: func(_ int) float64 {
				return rand.NormFloat64()*100 + 600 // shifted to avoid negatives
			},
		},
		{
			label: "Exponential",
			distribution: func(_ int) float64 {
				return rand.ExpFloat64() * 100
			},
		},
	}

	sketches := make([]*DDSketch, len(distributions))
	queryTimes := make([]float64, len(distributions))
	labels := make([]string, len(distributions))

	results := []map[string]interface{}{}

	for i, dist := range distributions {
		values := make([]float64, numValues)
		for j := 0; j < numValues; j++ {
			values[j] = dist.distribution(j)
		}
		sk, err := NewDDSketch(relAccuracy)
		if err != nil {
			t.Fatalf("Failed to create sketch: %v", err)
		}
		for _, v := range values {
			sk.Add(v)
		}
		sketches[i] = sk
		labels[i] = dist.label
		// Query time for this sketch
		startQuery := time.Now()
		for _, q := range quantiles {
			sk.Quantile(q)
		}
		queryTimes[i] = float64(time.Since(startQuery).Microseconds())

		// Compute quantile errors for this sketch
		sort.Float64s(values)
		errors := make([]float64, len(quantiles))
		for k, q := range quantiles {
			index := int(math.Floor(q * float64(len(values))))
			if index >= len(values) {
				index = len(values) - 1
			}
			exact := values[index]
			est := sk.Quantile(q)
			if math.Abs(exact) < 1e-9 {
				errors[k] = 0
			} else {
				errors[k] = math.Abs(est-exact) / math.Abs(exact)
			}
		}

		results = append(results, map[string]interface{}{
			"label":         labels[i],
			"query_time_us": queryTimes[i],
			"errors":        errors,
		})
	}

	// Merge all three sketches into a new sketch
	mergedSketch, err := NewDDSketch(relAccuracy)
	if err != nil {
		t.Fatalf("Failed to create merged sketch: %v", err)
	}
	for _, sk := range sketches {
		mergedSketch.Merge(sk)
	}
	startQueryMerged := time.Now()
	for _, q := range quantiles {
		mergedSketch.Quantile(q)
	}
	queryTimeMerged := float64(time.Since(startQueryMerged).Microseconds())

	// Compute quantile errors for merged sketch
	// Use all values from all distributions for ground truth
	allValues := []float64{}
	for _, dist := range distributions {
		for j := 0; j < numValues; j++ {
			allValues = append(allValues, dist.distribution(j))
		}
	}
	sort.Float64s(allValues)
	mergedErrors := make([]float64, len(quantiles))
	for i, q := range quantiles {
		index := int(math.Floor(q * float64(len(allValues))))
		if index >= len(allValues) {
			index = len(allValues) - 1
		}
		exact := allValues[index]
		est := mergedSketch.Quantile(q)
		if math.Abs(exact) < 1e-9 {
			mergedErrors[i] = 0
		} else {
			mergedErrors[i] = math.Abs(est-exact) / math.Abs(exact)
		}
	}

	results = append(results, map[string]interface{}{
		"label":         "Merged",
		"query_time_us": queryTimeMerged,
		"errors":        mergedErrors,
	})

	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal results: %v", err)
	}

	err = os.WriteFile("profiling_results/distribution_results.json", jsonData, 0644)
	if err != nil {
		t.Fatalf("Failed to write results file: %v", err)
	}
}
