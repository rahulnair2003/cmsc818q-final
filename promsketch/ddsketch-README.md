# DDSketch Static and Scenario Test Analysis

This folder contains the core DDSketch implementation and test suite for analyzing its performance under different scenarios.

## Core Components
- `ddsketch.go`: Core DDSketch implementation
- `ddsketch_test.go`: Test suite for analyzing DDSketch performance

## Running Tests

To run specific DDSketch test scenarios:
```bash
# Test basic DDSketch functionality
go test -v -run TestDDSketchBasic

# Test across different distributions
go test -v -run TestDDSketchAcrossDistributions

# Test with Google cluster data
go test -v -run TestDDSketchGoogleClusterData

# Test streaming analysis
go test -v -run TestDDSketchStreamingAnalysis
```

## Test Scenarios

The test suite analyzes DDSketch performance under various conditions:

1. **Basic Functionality**
   - Empty sketch properties
   - Adding values
   - Basic statistics (count, sum, min, max)
   - Quantile calculations
   - Merge operations
   - Reset functionality

2. **Distribution Analysis**
   - Uniform distribution
   - Normal distribution
   - Exponential distribution
   - Real-world data (Google cluster metrics)

3. **Performance Tests**
   - Streaming data analysis
   - Merge operations
   - Memory usage
   - Query latency

## Output Format
Tests output:
- Distribution statistics
- Quantile values and errors
- Performance measurements
- Memory usage statistics


4. **Manually add required data files:**
   - Download or copy the following files into the `promsketch/testdata/` directory:
     - `google-cluster-data-1.csv`
     - `household_power_consumption.txt`
   - These files are not included in the repository due to GitHub's 100MB file size limit.
5. Run the specific DDSketch tests as described below. 
