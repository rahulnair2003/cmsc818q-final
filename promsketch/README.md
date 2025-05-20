# promsketch

This repository provides PromSketch package for Prometheus and VictoriaMetrics.


### Install Dependencies
```
# installs Golang
wget https://go.dev/dl/go1.22.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

```
# installs nvm (Node Version Manager)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash
# download and install Node.js (you may need to restart the terminal)
nvm install 20
```

### Datasets
* Goolge Cluster Data v1: https://github.com/google/cluster-data/blob/master/TraceVersion1.md
* Power dataset: https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set?resource=download
* CAIDA traces: https://www.caida.org/catalog/datasets/passive_dataset_download/

### Run EHUniv test
```
cd promsketch
go test -v -timeout 0 -run ^TestExpoHistogramUnivMonOptimizedCAIDA$ github.com/froot/promsketch
```

### Run EHKLL test
```
cd promsketch
go test -v -timeout 0 -run ^TestCostAnalysisQuantile$ github.com/froot/promsketch
```

### Integration with Prometheus

```
git clone git@github.com:zzylol/prometheus-sketches.git
```
Compile:
```
cd prometheus-sketches
make build
```

### Integration with VictoriaMetrics single-node version

```
git clone git@github.com:zzylol/VictoriaMetrics.git
```
Compile:
```
cd VictoriaMetrics
make victoria-metrics
make vmalert
```

### Integration with VictoriaMetrics Cluster version
https://github.com/zzylol/VictoriaMetrics-cluster

## Setup Instructions

1. Clone this repository:
   ```sh
   git clone https://github.com/rahulnair2003/cmsc818q.git
   ```
2. Install Go (version 1.18+ recommended).
3. Install dependencies:
   ```sh
   cd promsketch
   go mod download
   ```
4. **Manually add required data files:**
   - Download or copy the following files into the `promsketch/testdata/` directory:
     - `google-cluster-data-1.csv`
     - `household_power_consumption.txt`
   - These files are not included in the repository due to GitHub's 100MB file size limit.
5. Run tests or benchmarks as described below.
