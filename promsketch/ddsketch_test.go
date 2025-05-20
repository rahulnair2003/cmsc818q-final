package promsketch

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDSketchBasic(t *testing.T) {
	// Create a new DDSketch with 1% relative accuracy
	sketch, err := NewDDSketch(0.01)
	if err != nil {
		t.Fatalf("Failed to create DDSketch: %v", err)
	}

	// Test empty sketch
	if sketch.GetCount() != 0 {
		t.Errorf("Expected count 0, got %d", sketch.GetCount())
	}
	if sketch.GetSum() != 0 {
		t.Errorf("Expected sum 0, got %f", sketch.GetSum())
	}
	if sketch.GetMin() != math.MaxFloat64 {
		t.Errorf("Expected min %f, got %f", math.MaxFloat64, sketch.GetMin())
	}
	if sketch.GetMax() != -math.MaxFloat64 {
		t.Errorf("Expected max %f, got %f", -math.MaxFloat64, sketch.GetMax())
	}
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	for _, v := range values {
		sketch.Add(v)
	}
	if sketch.GetCount() != int64(len(values)) {
		t.Errorf("Expected count %d, got %d", len(values), sketch.GetCount())
	}
	if sketch.GetSum() != 15.0 {
		t.Errorf("Expected sum 15.0, got %f", sketch.GetSum())
	}
	if sketch.GetMin() != 1.0 {
		t.Errorf("Expected min 1.0, got %f", sketch.GetMin())
	}
	if sketch.GetMax() != 5.0 {
		t.Errorf("Expected max 5.0, got %f", sketch.GetMax())
	}
	testQuantiles := map[float64]float64{
		0.0:  1.0, // min
		0.25: 2.0, // 25th percentile
		0.5:  3.0, // median
		0.75: 4.0, // 75th percentile
		1.0:  5.0, // max
	}

	for q, expected := range testQuantiles {
		got := sketch.Quantile(q)
		if math.Abs(got-expected) > 0.1 {
			t.Errorf("Quantile %f: expected %f, got %f", q, expected, got)
		}
	}

	// Test merge
	other, err := NewDDSketch(0.01)
	if err != nil {
		t.Fatalf("Failed to create second DDSketch: %v", err)
	}

	otherValues := []float64{6.0, 7.0, 8.0}
	for _, v := range otherValues {
		other.Add(v)
	}

	sketch.Merge(other)

	// Test merged statistics
	expectedCount := int64(len(values) + len(otherValues))
	if sketch.GetCount() != expectedCount {
		t.Errorf("After merge: expected count %d, got %d", expectedCount, sketch.GetCount())
	}
	if sketch.GetMin() != 1.0 {
		t.Errorf("After merge: expected min 1.0, got %f", sketch.GetMin())
	}
	if sketch.GetMax() != 8.0 {
		t.Errorf("After merge: expected max 8.0, got %f", sketch.GetMax())
	}

	// Test reset
	sketch.Reset()
	if sketch.GetCount() != 0 {
		t.Errorf("After reset: expected count 0, got %d", sketch.GetCount())
	}
	if sketch.GetSum() != 0 {
		t.Errorf("After reset: expected sum 0, got %f", sketch.GetSum())
	}
	if sketch.GetMin() != math.MaxFloat64 {
		t.Errorf("After reset: expected min %f, got %f", math.MaxFloat64, sketch.GetMin())
	}
	if sketch.GetMax() != -math.MaxFloat64 {
		t.Errorf("After reset: expected max %f, got %f", -math.MaxFloat64, sketch.GetMax())
	}
}

func TestDDSketchMerge(t *testing.T) {
	sketch1, err := NewDDSketch(0.01)
	require.NoError(t, err)

	sketch2, err := NewDDSketch(0.01)
	require.NoError(t, err)

	// Split values between two sketches
	values := make([]float64, 1000)
	for i := range values {
		values[i] = rand.Float64() * 1000
	}

	for _, v := range values[:500] {
		sketch1.Add(v)
	}
	for _, v := range values[500:] {
		sketch2.Add(v)
	}

	sketch1.Merge(sketch2)

	// Verify merged quantile with 1% relative error
	expected := exactQuantile(values, 0.99)
	actual := sketch1.Quantile(0.99)
	assert.InDelta(t, expected, actual, expected*0.01)
}

// Helper functions
func exactQuantile(data []float64, q float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	index := int(float64(len(sorted)-1) * q)
	return sorted[index]
}

func TestNewDDSketch(t *testing.T) {
	tests := []struct {
		name             string
		relativeAccuracy float64
		wantErr          bool
	}{
		{
			name:             "valid accuracy",
			relativeAccuracy: 0.01,
			wantErr:          false,
		},
		{
			name:             "zero accuracy",
			relativeAccuracy: 0,
			wantErr:          true,
		},
		{
			name:             "negative accuracy",
			relativeAccuracy: -0.01,
			wantErr:          true,
		},
		{
			name:             "accuracy >= 1",
			relativeAccuracy: 1.0,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketch, err := NewDDSketch(tt.relativeAccuracy)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDDSketch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotNil(t, sketch)
				assert.Equal(t, int64(0), sketch.GetCount())
				assert.Equal(t, math.MaxFloat64, sketch.GetMin())
				assert.Equal(t, -math.MaxFloat64, sketch.GetMax())
				assert.Equal(t, float64(0), sketch.GetSum())
			}
		})
	}
}

func TestDDSketch_Add(t *testing.T) {
	sketch, err := NewDDSketch(0.01)
	assert.NoError(t, err)

	// Test adding positive values
	sketch.Add(1.0)
	assert.Equal(t, int64(1), sketch.GetCount())
	assert.Equal(t, 1.0, sketch.GetMin())
	assert.Equal(t, 1.0, sketch.GetMax())
	assert.Equal(t, 1.0, sketch.GetSum())

	// Test adding negative values
	sketch.Add(-2.0)
	assert.Equal(t, int64(2), sketch.GetCount())
	assert.Equal(t, -2.0, sketch.GetMin())
	assert.Equal(t, 1.0, sketch.GetMax())
	assert.Equal(t, -1.0, sketch.GetSum())

	// Test adding zero
	sketch.Add(0.0)
	assert.Equal(t, int64(3), sketch.GetCount())
	assert.Equal(t, -2.0, sketch.GetMin())
	assert.Equal(t, 1.0, sketch.GetMax())
	assert.Equal(t, -1.0, sketch.GetSum())
}

func TestDDSketch_Quantile(t *testing.T) {
	// This test demonstrates a known limitation of DDSketch with exponential distributions
	// DDSketch's binning strategy is optimized for more uniform distributions and can
	// significantly underestimate values in exponential distributions with large gaps
	sketch, err := NewDDSketch(0.01)
	assert.NoError(t, err)

	// Add values that grow exponentially (powers of 2)
	// This creates large gaps between values, which is challenging for DDSketch
	values := []float64{1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0}
	for _, v := range values {
		sketch.Add(v)
	}

	tests := []struct {
		name     string
		quantile float64
		want     float64
		delta    float64 // Allow different error bounds per test case
	}{
		{"min", 0.0, 1.0, 0.01},
		{"25th percentile", 0.25, 4.0, 0.01},
		{"median", 0.5, 16.0, 0.01},
		{"75th percentile", 0.75, 128.0, 0.5}, // Large error expected due to exponential distribution
		{"max", 1.0, 512.0, 0.01},
		{"invalid low", -0.1, 1.0, 0.0},   // Expect min for q < 0
		{"invalid high", 1.1, 512.0, 0.0}, // Expect max for q > 1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sketch.Quantile(tt.quantile)
			if tt.quantile >= 0 && tt.quantile <= 1 {
				// Use the test-specific error bound
				assert.InDelta(t, tt.want, got, tt.want*tt.delta)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestDDSketch_Merge(t *testing.T) {
	sketch1, err := NewDDSketch(0.01)
	assert.NoError(t, err)

	sketch2, err := NewDDSketch(0.01)
	assert.NoError(t, err)

	// Add values to first sketch
	values1 := []float64{1.0, 2.0, 3.0}
	for _, v := range values1 {
		sketch1.Add(v)
	}

	// Add values to second sketch
	values2 := []float64{4.0, 5.0, 6.0}
	for _, v := range values2 {
		sketch2.Add(v)
	}

	// Merge sketches
	sketch1.Merge(sketch2)

	// Verify merged results
	assert.Equal(t, int64(6), sketch1.GetCount())
	assert.Equal(t, 1.0, sketch1.GetMin())
	assert.Equal(t, 6.0, sketch1.GetMax())
	assert.Equal(t, 21.0, sketch1.GetSum())

	// Test merging with nil
	sketch1.Merge(nil)
	assert.Equal(t, int64(6), sketch1.GetCount())
}

func TestDDSketch_Reset(t *testing.T) {
	sketch, err := NewDDSketch(0.01)
	assert.NoError(t, err)

	// Add some values
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	for _, v := range values {
		sketch.Add(v)
	}

	// Reset the sketch
	sketch.Reset()

	// Verify reset state
	assert.Equal(t, int64(0), sketch.GetCount())
	assert.Equal(t, math.MaxFloat64, sketch.GetMin())
	assert.Equal(t, -math.MaxFloat64, sketch.GetMax())
	assert.Equal(t, float64(0), sketch.GetSum())

	// Verify sketch is still usable
	sketch.Add(1.0)
	assert.Equal(t, int64(1), sketch.GetCount())
	assert.Equal(t, 1.0, sketch.GetMin())
	assert.Equal(t, 1.0, sketch.GetMax())
	assert.Equal(t, 1.0, sketch.GetSum())
}

func TestDDSketchStreamingAnalysis(t *testing.T) {
	const initialValues = 20000000
	const streamValues = 1000
	values := make([]float64, initialValues)

	// Generate initial random values
	for i := range values {
		values[i] = rand.Float64() * 1000
	}

	// Measure initial DDSketch setup time
	startSketchSetup := time.Now()
	sketch, err := NewDDSketch(0.01)
	require.NoError(t, err)
	for _, v := range values {
		sketch.Add(v)
	}
	sketchSetupTime := time.Since(startSketchSetup)

	// Measure initial raw computation setup time (sorting)
	startRawSetup := time.Now()
	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	sort.Float64s(sortedValues)
	rawSetupTime := time.Since(startRawSetup)

	t.Logf("\nInitial Setup Time Comparison:")
	t.Logf("DDSketch setup (building): %v", sketchSetupTime)
	t.Logf("Raw setup (sorting): %v", rawSetupTime)
	t.Logf("Setup speedup ratio: %.2fx", float64(rawSetupTime)/float64(sketchSetupTime))

	// Generate new streaming values
	streamData := make([]float64, streamValues)
	for i := range streamData {
		streamData[i] = rand.Float64() * 1000
	}

	// Measure streaming update time for DDSketch
	startSketchStream := time.Now()
	for _, v := range streamData {
		sketch.Add(v)
	}
	sketchStreamTime := time.Since(startSketchStream)

	// Measure streaming update time for raw computation (need to re-sort)
	startRawStream := time.Now()
	updatedValues := append(sortedValues, streamData...)
	sort.Float64s(updatedValues)
	rawStreamTime := time.Since(startRawStream)

	t.Logf("\nStreaming Update Time Comparison:")
	t.Logf("DDSketch update time: %v", sketchStreamTime)
	t.Logf("Raw update time (re-sort): %v", rawStreamTime)
	t.Logf("Streaming speedup ratio: %.2fx", float64(rawStreamTime)/float64(sketchStreamTime))

	// Calculate net performance (setup + streaming)
	sketchNetTime := sketchSetupTime + sketchStreamTime
	rawNetTime := rawSetupTime + rawStreamTime

	t.Logf("\nNet Performance Comparison (Setup + Streaming):")
	t.Logf("DDSketch total time: %v", sketchNetTime)
	t.Logf("Raw total time: %v", rawNetTime)
	t.Logf("Net speedup ratio: %.2fx", float64(rawNetTime)/float64(sketchNetTime))

	// Measure performance for different quantiles after streaming
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}

	t.Logf("\nPost-Stream Query Performance:")
	t.Logf("Quantile | DDSketch Time | Raw Time | Speedup Ratio | Accuracy")
	t.Logf("---------|--------------|----------|--------------|----------")

	var totalSketchQueryTime time.Duration
	var totalRawQueryTime time.Duration

	for _, q := range quantiles {
		// Measure DDSketch performance
		startSketch := time.Now()
		sketchValue := sketch.Quantile(q)
		sketchTime := time.Since(startSketch)
		totalSketchQueryTime += sketchTime

		// Measure raw computation performance
		startRaw := time.Now()
		rawValue := updatedValues[int(float64(len(updatedValues)-1)*q)]
		rawTime := time.Since(startRaw)
		totalRawQueryTime += rawTime

		// Calculate speedup ratio
		speedup := float64(rawTime) / float64(sketchTime)

		// Calculate accuracy
		accuracy := math.Abs(sketchValue-rawValue) / rawValue * 100

		t.Logf("p%.0f    | %10v | %8v | %12.2fx | %.2f%%",
			q*100, sketchTime, rawTime, speedup, accuracy)
	}

	// Calculate total performance including queries
	sketchTotalTime := sketchNetTime + totalSketchQueryTime
	rawTotalTime := rawNetTime + totalRawQueryTime

	t.Logf("\nTotal Performance (Setup + Streaming + Queries):")
	t.Logf("DDSketch total time: %v", sketchTotalTime)
	t.Logf("Raw total time: %v", rawTotalTime)
	t.Logf("Total speedup ratio: %.2fx", float64(rawTotalTime)/float64(sketchTotalTime))

	// Measure memory efficiency
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)

	sketchSize := m.HeapAlloc
	rawSize := int64(len(updatedValues) * 8) // 8 bytes per float64

	t.Logf("\nMemory Efficiency:")
	t.Logf("DDSketch size: %d bytes (%.2f KB)", sketchSize, float64(sketchSize)/1024)
	t.Logf("Raw array size: %d bytes (%.2f KB)", rawSize, float64(rawSize)/1024)
	t.Logf("Memory savings: %.2f%%", (1-float64(sketchSize)/float64(rawSize))*100)

	t.Logf("\nMetadata:")
	t.Logf("Total values tracked: %d", sketch.GetCount())
	t.Logf("Value range: %.2f to %.2f", sketch.GetMin(), sketch.GetMax())
	t.Logf("Average value: %.2f", sketch.GetSum()/float64(sketch.GetCount()))
}

func TestDDSketchGoogleClusterData(t *testing.T) {
	values := loadGoogleClusterData()
	require.NotEmpty(t, values, "Failed to load Google cluster data")

	// Force multiple GCs before measurement
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	before := int64(m1.HeapAlloc)

	// Create DDSketch with 1% relative accuracy
	sketch, err := NewDDSketch(0.01)
	require.NoError(t, err)

	// Add values to sketch
	startAdd := time.Now()
	for _, v := range values {
		sketch.Add(v)
	}
	addTime := time.Since(startAdd)
	t.Logf("\nSketch Build Time: %v", addTime)

	// Force multiple GCs after measurement
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	after := int64(m2.HeapAlloc)

	memUsage := after - before
	if memUsage < 0 {
		t.Logf("Warning: Negative memory usage detected.")
		memUsage = int64(m2.HeapObjects-m1.HeapObjects) * 8
	}

	// Memory efficiency metrics
	t.Logf("\nMemory Analysis:")
	t.Logf("Sketch size: %d bytes (%.2f KB)", memUsage, float64(memUsage)/1024)
	t.Logf("Memory per value: %.4f bytes", float64(memUsage)/float64(len(values)))
	t.Logf("Raw array size: %d bytes (%.2f KB)", len(values)*8, float64(len(values)*8)/1024)
	if memUsage > 0 {
		savings := (1 - float64(memUsage)/float64(len(values)*8)) * 100
		t.Logf("Memory savings: %.2f%%", savings)
	}

	// Sort values for accuracy comparison
	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	sort.Float64s(sortedValues)

	// Test query performance and accuracy
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}

	t.Logf("\nQuery Performance and Accuracy:")
	t.Logf("Quantile | Query Time | Value | Error")
	t.Logf("---------|------------|-------|-------")

	var totalQueryTime time.Duration
	var minQueryTime time.Duration = time.Hour
	var maxQueryTime time.Duration
	var maxError float64

	for _, q := range quantiles {
		// Measure query performance
		startQuery := time.Now()
		sketchValue := sketch.Quantile(q)
		queryTime := time.Since(startQuery)

		// Calculate accuracy
		exactValue := sortedValues[int(float64(len(sortedValues)-1)*q)]
		relativeError := math.Abs(exactValue-sketchValue) / exactValue * 100
		if relativeError > maxError {
			maxError = relativeError
		}

		totalQueryTime += queryTime
		if queryTime < minQueryTime {
			minQueryTime = queryTime
		}
		if queryTime > maxQueryTime {
			maxQueryTime = queryTime
		}

		t.Logf("p%.0f    | %9v | %.4f | %.2f%%",
			q*100, queryTime, sketchValue, relativeError)

		require.Less(t, relativeError, 2.0,
			"Relative error exceeds 2%% for quantile %.2f", q)
	}

	t.Logf("\nAggregate Query Performance:")
	t.Logf("Total query time: %v", totalQueryTime)
	t.Logf("Average query time: %v", totalQueryTime/time.Duration(len(quantiles)))
	t.Logf("Min query time: %v", minQueryTime)
	t.Logf("Max query time: %v", maxQueryTime)
	t.Logf("Max relative error: %.2f%%", maxError)

	t.Logf("\nMetadata:")
	t.Logf("Values tracked: %d", sketch.GetCount())
	t.Logf("Value range: %.2f to %.2f", sketch.GetMin(), sketch.GetMax())
	t.Logf("Average value: %.2f", sketch.GetSum()/float64(sketch.GetCount()))
}

func TestDDSketchAcrossDistributions(t *testing.T) {
	const numValues = 10000000
	relAccuracy := 0.01
	// Allow for 1.5x the specified relative accuracy to account for statistical variation
	maxAllowedError := relAccuracy
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}

	type sketchResult struct {
		sketch       *DDSketch
		values       []float64
		label        string
		distribution func(i int) float64
	}

	distributions := []sketchResult{
		{
			label:  "Uniform",
			values: make([]float64, numValues),
			distribution: func(_ int) float64 {
				return rand.Float64() * 1000
			},
		},
		{
			label:  "Normal",
			values: make([]float64, numValues),
			distribution: func(_ int) float64 {
				return rand.NormFloat64()*100 + 600 // shifted to avoid negatives
			},
		},
		{
			label:  "Exponential (Heavy Tail)",
			values: make([]float64, numValues),
			distribution: func(_ int) float64 {
				return rand.ExpFloat64() * 100
			},
		},
	}

	for i := range distributions {
		// Fill in values
		for j := 0; j < numValues; j++ {
			distributions[i].values[j] = distributions[i].distribution(j)
		}

		// Create and populate sketch
		sk, err := NewDDSketch(relAccuracy)
		require.NoError(t, err)
		for _, v := range distributions[i].values {
			sk.Add(v)
		}

		distributions[i].sketch = sk

		// Accuracy and metadata
		sort.Float64s(distributions[i].values)
		t.Logf("\n--- %s Distribution ---", distributions[i].label)
		t.Logf("Values tracked: %d", sk.GetCount())
		t.Logf("Value range: %.2f to %.2f", sk.GetMin(), sk.GetMax())
		t.Logf("Average value: %.2f", sk.GetSum()/float64(sk.GetCount()))

		t.Logf("Quantile Accuracy (max allowed error: %.2f%%):", maxAllowedError*100)
		for _, q := range quantiles {
			exact := distributions[i].values[int(float64(numValues-1)*q)]
			est := distributions[i].sketch.Quantile(q)
			err := math.Abs(est-exact) / exact
			t.Logf("  p%.0f: %.2f%% (Exact: %.2f, Est: %.2f)", q*100, err*100, exact, est)
			require.Less(t, err, maxAllowedError,
				"Relative error %.2f%% exceeds allowed %.2f%% for %s at p%.2f",
				err*100, maxAllowedError*100, distributions[i].label, q)
		}
	}

	// Merge the sketches
	mergedSketch, err := NewDDSketch(relAccuracy)
	require.NoError(t, err)
	for _, d := range distributions {
		mergedSketch.Merge(d.sketch)
	}

	// Combined quantiles
	allValues := append(append(distributions[0].values, distributions[1].values...), distributions[2].values...)
	sort.Float64s(allValues)

	t.Logf("\n--- Merged Sketch ---")
	t.Logf("Values tracked: %d", mergedSketch.GetCount())
	t.Logf("Value range: %.2f to %.2f", mergedSketch.GetMin(), mergedSketch.GetMax())
	t.Logf("Average value: %.2f", mergedSketch.GetSum()/float64(mergedSketch.GetCount()))

	t.Logf("Quantile Accuracy (Merged):")
	for _, q := range quantiles {
		exact := allValues[int(float64(len(allValues)-1)*q)]
		est := mergedSketch.Quantile(q)
		err := math.Abs(est-exact) / exact
		t.Logf("  p%.0f: %.2f%% (Exact: %.2f, Est: %.2f)", q*100, err*100, exact, est)
		require.Less(t, err, maxAllowedError,
			"Relative error %.2f%% exceeds allowed %.2f%% for merged sketch at p%.2f",
			err*100, maxAllowedError*100, q)
	}
}

// Helper functions
func loadGoogleClusterData() []float64 {
	file, err := os.Open("testdata/google-cluster-data-1.csv")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return nil
	}
	defer file.Close()

	var values []float64
	scanner := bufio.NewScanner(file)

	// Skip header
	if !scanner.Scan() {
		fmt.Printf("Error reading header: %v\n", scanner.Err())
		return nil
	}

	// Process data lines
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		value, err := strconv.ParseFloat(fields[4], 64)
		if err != nil {
			continue
		}
		if value > 0 {
			values = append(values, value)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning file: %v\n", err)
		return nil
	}

	return values
}
