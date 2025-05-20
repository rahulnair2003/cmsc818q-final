# CMSC818Q Quantile Sketch Project

## Setup Instructions

1. Clone this repository:
   ```sh
   git clone https://github.com/rahulnair2003/cmsc818q-final.git
   ```
2. Install Go (version 1.18+ recommended).

3. Manually add required data files:
   Download or copy the following files into the `promsketch/testdata/` directory:
     - `google-cluster-data-1.csv`
     - `household_power_consumption.txt`



### 1. DDSketch Tests
- **Location:** [promsketch/ddsketch-README.md](promsketch/ddsketch-README.md)
- **Description:**
  - Contains unit and integration tests for DDSketch, including merge, distribution, and streaming analysis.
  - Provides instructions for running specific tests and interpreting their output.

### 2. Prometheus Ingestion Pipeline
- **Location:** [promsketch-reciever/README.md](promsketch-reciever/README.md)
- **Description:**
  - Implements a Prometheus-compatible ingestion pipeline for quantile metrics using DDSketch.
  - Includes a test metrics exporter, ingestion server, and Prometheus integration.
  - Instructions for setup, running, querying, and troubleshooting are provided in the README.

### 3. Window Receiver Simulation
- **Location:** [window-receiver/simulated/README.md](window-receiver/simulated/README.md)
- **Description:**
  - Simulates a rotating buffer of DDSketches to analyze quantile accuracy and merge efficiency over sliding windows.
  - Includes performance measurement and configuration options.
  - See the README for usage and interpretation of results.

---

This repo has the code for my final project for CMSC818Q. For details on each component, see the linked READMEs above. - Rahul
