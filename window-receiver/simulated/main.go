package main

import (
	"fmt"
	"math"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
)

func generateData(counter int) float64 {
	// Generate incremental data from 1 to 5000
	return float64(counter)
}

func printSketchStats(nSketch *NSketchWindow, index int, startValue, endValue int) {
	quantiles := []float64{0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0}
	results, err := nSketch.activeSketches[index].sketch.GetValuesAtQuantiles(quantiles)
	if err != nil {
		fmt.Printf("Error getting quantiles for sketch %d: %v\n", index, err)
		return
	}
	fmt.Printf("\nSketch %d Stats (values %d-%d):\n", index, startValue, endValue)
	fmt.Printf("  Quantiles:\n")
	for i, q := range quantiles {
		expected := float64(startValue) + q*float64(endValue-startValue)
		relError := math.Abs(results[i]-expected) / expected
		fmt.Printf("    p%.0f: %f (rel error: %.2f%%)\n", q*100, results[i], relError*100)
	}
	fmt.Printf("  Count: %d\n", nSketch.activeSketches[index].count)
	fmt.Printf("  Min: %f\n", nSketch.activeSketches[index].min)
	fmt.Printf("  Max: %f\n", nSketch.activeSketches[index].max)
}

func printMergedStatsN(nSketch *NSketchWindow, indices []int, starts, ends []int) {
	if len(indices) == 0 {
		return
	}
	tempSketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		fmt.Printf("Error creating temporary sketch: %v\n", err)
		return
	}
	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	count := int64(0)
	mergedStart := starts[0]
	mergedEnd := ends[0]
	for i, idx := range indices {
		sk := nSketch.activeSketches[idx]
		if err := tempSketch.MergeWith(sk.sketch); err != nil {
			fmt.Printf("Error merging sketch %d: %v\n", idx, err)
			return
		}
		if sk.min < minVal {
			minVal = sk.min
		}
		if sk.max > maxVal {
			maxVal = sk.max
		}
		count += sk.count
		if starts[i] < mergedStart {
			mergedStart = starts[i]
		}
		if ends[i] > mergedEnd {
			mergedEnd = ends[i]
		}
	}
	quantiles := []float64{0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0}
	results, err := tempSketch.GetValuesAtQuantiles(quantiles)
	if err != nil {
		fmt.Printf("Error getting quantiles for merged sketch: %v\n", err)
		return
	}
	fmt.Printf("\nMerged Stats (values %d-%d, %d sketches):\n", mergedStart, mergedEnd, len(indices))
	fmt.Printf("  Quantiles:\n")
	for i, q := range quantiles {
		expected := float64(mergedStart) + q*float64(mergedEnd-mergedStart)
		relError := math.Abs(results[i]-expected) / expected
		fmt.Printf("    p%.0f: %f (rel error: %.2f%%)\n", q*100, results[i], relError*100)
	}
	fmt.Printf("  Total Count: %d\n", count)
	fmt.Printf("  Min: %f\n", minVal)
	fmt.Printf("  Max: %f\n", maxVal)
}

func main() {
	windowSize := 100000000
	nSketches := 24
	mergeCount := 3
	maxValue := 2400000000

	nSketch, err := NewNSketchWindow(windowSize, nSketches, 0.01)
	if err != nil {
		fmt.Println("Error initializing NSketchWindow:", err)
		return
	}

	for i := 1; i <= maxValue; i++ {
		nSketch.Add(generateData(i))

		if i >= windowSize*mergeCount && i%windowSize == 0 {
			indices := make([]int, mergeCount)
			starts := make([]int, mergeCount)
			ends := make([]int, mergeCount)
			for j := 0; j < mergeCount; j++ {
				idx := ((i / windowSize) - mergeCount + j) % nSketches
				indices[j] = idx
				starts[j] = idx*windowSize + 1
				ends[j] = starts[j] + windowSize - 1
				printSketchStats(nSketch, idx, starts[j], ends[j])
			}
			start := time.Now()
			printMergedStatsN(nSketch, indices, starts, ends)
			elapsed := time.Since(start)
			fmt.Printf("  Merge Time: %s\n", elapsed)
		}
	}

	fmt.Println("\n=== Final Stats ===")
	for i := 0; i < nSketches; i++ {
		startValue := (i * windowSize) + 1
		endValue := startValue + windowSize - 1
		printSketchStats(nSketch, i, startValue, endValue)
	}

	fmt.Println("\n=== Overall Stats ===")
	fmt.Printf("Current Window Index: %d\n", nSketch.GetCurrentIndex())
	fmt.Printf("Total Count: %d\n", nSketch.GetTotalCount())
	fmt.Printf("Min Value: %f\n", nSketch.GetMin())
	fmt.Printf("Max Value: %f\n", nSketch.GetMax())
}
