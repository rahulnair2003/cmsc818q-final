package main

import (
	"fmt"
	"math"

	"github.com/DataDog/sketches-go/ddsketch"
)

type DDSketch struct {
	sketch *ddsketch.DDSketch
	count  int64
	min    float64
	max    float64
	sum    float64
}

// NewDDSketch creates a new DDSketch with the specified relative accuracy
func NewDDSketch(relativeAccuracy float64) (*DDSketch, error) {
	if relativeAccuracy <= 0 || relativeAccuracy >= 1 {
		return nil, fmt.Errorf("relative accuracy must be between 0 and 1, got %f", relativeAccuracy)
	}

	sketch, err := ddsketch.NewDefaultDDSketch(relativeAccuracy)
	if err != nil {
		return nil, fmt.Errorf("failed to create DDSketch: %w", err)
	}

	return &DDSketch{
		sketch: sketch,
		min:    math.MaxFloat64,
		max:    -math.MaxFloat64,
	}, nil
}

func (d *DDSketch) Add(value float64) {
	// Skip NaN and infinite values
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}

	if err := d.sketch.Add(value); err != nil {
		fmt.Printf("Warning: failed to add value to DDSketch: %v\n", err)
		return
	}

	d.count++
	d.sum += value

	if value < d.min {
		d.min = value
	}
	if value > d.max {
		d.max = value
	}
}

// Quantile returns the approximate value at the given quantile
func (d *DDSketch) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		fmt.Printf("Warning: quantile must be between 0 and 1, got %f\n", q)
		if q < 0 {
			return d.min
		}
		return d.max
	}

	if d.count == 0 {
		return 0
	}

	quantiles, err := d.sketch.GetValuesAtQuantiles([]float64{q})
	if err != nil {
		fmt.Printf("Warning: failed to get quantile: %v\n", err)
		if q <= 0.5 {
			return d.min
		}
		return d.max
	}

	return quantiles[0]
}

func (d *DDSketch) GetCount() int64 {
	return d.count
}

func (d *DDSketch) GetSum() float64 {
	return d.sum
}

func (d *DDSketch) GetMin() float64 {
	return d.min
}

func (d *DDSketch) GetMax() float64 {
	return d.max
}

// Merge merges another DDSketch into this one
func (d *DDSketch) Merge(other *DDSketch) {
	if other == nil {
		return
	}

	if err := d.sketch.MergeWith(other.sketch); err != nil {
		fmt.Printf("Warning: failed to merge sketches: %v\n", err)
		return
	}

	d.count += other.count
	d.sum += other.sum

	if other.min < d.min {
		d.min = other.min
	}
	if other.max > d.max {
		d.max = other.max
	}
}
