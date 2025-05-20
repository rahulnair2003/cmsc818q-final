# Sliding Window-DDSketch Proof of Concept

This simulation demonstrates how DDSketch's merge capabilities can be leveraged for time-relevant applications requiring sliding window quantile analysis. The system maintains a rotating buffer of sketches, allowing efficient merging of historical data while maintaining accurate quantile estimates.

## Key Features
- Sliding window quantile analysis
- Efficient sketch merging
- Configurable window size and merge parameters
- Synthetic data generation for testing

## Configuration
The main parameters can be configured in `main.go` lines 88-91:

```go
windowSize := 100000000  // Values per sketch
nSketches := 24         // Number of sketches in the window
mergeCount := 3         // Number of sketches to merge
maxValue := 2400000000  // Total data points to process
```

## Running the Simulation
```bash
go run ddsketch_wrapper.go main.go
```

## Output
The simulation provides:
- Per-sketch statistics (count, min, max, quantiles)
- Merge operation timing
- Relative error measurements
- Overall performance metrics

## Use Cases
- Real-time monitoring systems
- Time-series data analysis
- Performance metrics tracking over time

## Performance
- Efficient memory usage through sketch merging
- Constant-time query operations
- Configurable sketch tuning vs. memory trade-off 