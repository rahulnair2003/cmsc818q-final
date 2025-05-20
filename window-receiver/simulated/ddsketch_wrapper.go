package main

import (
	"fmt"
	"math"

	"github.com/DataDog/sketches-go/ddsketch"
)

type DDSketchWrapper struct {
	sketch *ddsketch.DDSketch
	count  int64
	min    float64
	max    float64
	sum    float64
}

func NewDDSketchWrapper(relativeAccuracy float64) (*DDSketchWrapper, error) {
	sketch, err := ddsketch.NewDefaultDDSketch(relativeAccuracy)
	if err != nil {
		return nil, fmt.Errorf("failed to create DDSketch: %w", err)
	}

	return &DDSketchWrapper{
		sketch: sketch,
		min:    math.MaxFloat64,
		max:    -math.MaxFloat64,
	}, nil
}

type NSketchWindow struct {
	windowSize       int                // Number of values per window
	relativeAccuracy float64            // Relative accuracy for DDSketch
	activeSketches   []*DDSketchWrapper // Slice to store the n most recent DDSketches
	currentIndex     int                // Index of the current active sketch (for adding new data)
}

// NewNSketchWindow initializes the n DDSketches to hold the most recent n windows
func NewNSketchWindow(windowSize, n int, relativeAccuracy float64) (*NSketchWindow, error) {
	if windowSize <= 0 || n <= 0 {
		return nil, fmt.Errorf("windowSize and n must be greater than 0")
	}

	// Create a slice of n DDSketches to maintain the most recent n windows
	sketches := make([]*DDSketchWrapper, n)
	for i := 0; i < n; i++ {
		sketch, err := NewDDSketchWrapper(relativeAccuracy)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDSketch: %w", err)
		}
		sketches[i] = sketch
	}

	return &NSketchWindow{
		windowSize:       windowSize,
		relativeAccuracy: relativeAccuracy,
		activeSketches:   sketches,
		currentIndex:     0,
	}, nil
}

// Add a new value to the current active sketch
func (n *NSketchWindow) Add(value float64) {
	// Add the value to the current active sketch
	if err := n.activeSketches[n.currentIndex].sketch.Add(value); err != nil {
		fmt.Printf("Warning: failed to add value to DDSketch: %v\n", err)
		return
	}

	n.activeSketches[n.currentIndex].count++
	n.activeSketches[n.currentIndex].sum += value

	if value < n.activeSketches[n.currentIndex].min {
		n.activeSketches[n.currentIndex].min = value
	}
	if value > n.activeSketches[n.currentIndex].max {
		n.activeSketches[n.currentIndex].max = value
	}

	// If the current sketch has reached the window size, rotate the sketches
	if n.activeSketches[n.currentIndex].count >= int64(n.windowSize) {
		// Move to the next sketch
		n.currentIndex = (n.currentIndex + 1) % len(n.activeSketches)
		// Reset the sketch at the new index
		n.activeSketches[n.currentIndex].sketch, _ = ddsketch.NewDefaultDDSketch(n.relativeAccuracy)
		n.activeSketches[n.currentIndex].count = 0
		n.activeSketches[n.currentIndex].sum = 0
		n.activeSketches[n.currentIndex].min = math.MaxFloat64
		n.activeSketches[n.currentIndex].max = -math.MaxFloat64
	}
}

// Quantile queries the quantiles of the combined sketch of all windows
func (n *NSketchWindow) Quantile(qs []float64) ([]float64, error) {
	// Combine all sketches and retrieve values at specified quantiles
	quantiles, err := n.activeSketches[n.currentIndex].sketch.GetValuesAtQuantiles(qs)
	if err != nil {
		return nil, fmt.Errorf("failed to get quantiles: %v", err)
	}
	return quantiles, nil
}

// Get the current sketch index
func (n *NSketchWindow) GetCurrentIndex() int {
	return n.currentIndex
}

// Get the total count of all sketches
func (n *NSketchWindow) GetTotalCount() int64 {
	var totalCount int64
	for _, sketch := range n.activeSketches {
		totalCount += sketch.count
	}
	return totalCount
}

// Get the total sum of all sketches
func (n *NSketchWindow) GetTotalSum() float64 {
	var totalSum float64
	for _, sketch := range n.activeSketches {
		totalSum += sketch.sum
	}
	return totalSum
}

// Get the minimum value from all sketches
func (n *NSketchWindow) GetMin() float64 {
	min := math.MaxFloat64
	for _, sketch := range n.activeSketches {
		if sketch.min < min {
			min = sketch.min
		}
	}
	return min
}

// Get the maximum value from all sketches
func (n *NSketchWindow) GetMax() float64 {
	max := -math.MaxFloat64
	for _, sketch := range n.activeSketches {
		if sketch.max > max {
			max = sketch.max
		}
	}
	return max
}
