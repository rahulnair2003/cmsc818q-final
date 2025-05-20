# Prometheus Ingestion Pipeline

This folder contains the Prometheus ingestion pipeline for quantile metrics.

## Prerequisites
Install Prometheus:
```bash
# For macOS
brew install prometheus

# For Linux
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xvfz prometheus-*.tar.gz
cd prometheus-*
```

## Components

The system consists of three components running on different servers:
- **Test Exporter** (Port 9101): Generates test metrics with different distributions (uniform, normal, exponential)
- **Ingestion Server** (Port 8080): Processes incoming metrics and maintains DDSketch for quantile estimation
- **Prometheus** (Port 9090): Collects and stores metrics from the ingestion server

## Running the System

You'll need 4 terminal windows with the present working directory being promsketch-receiver:


1. Start the test exporter:
```bash
go run test_exporter.go
```

2. Start the ingestion server:
```bash
go run ddsketch.go main.go
```

3. Start Prometheus collector:
```bash
prometheus --config.file=promsketch-reciever/prometheus.yml
```

## Querying Endpoints

You can query quantiles and run speed tests using curl:

1. Query quantiles:
```bash
curl -s "http://localhost:8080/quantile?metric=test_exponential&q=0.95"
```

2. Run speed test:
```bash
curl -s "http://localhost:8080/speedtest?metric=test_exponential&duration=10"
```

## Troubleshooting

If you encounter errors like `address already in use` when starting any component, you may have lingering processes using the required ports. You can kill all processes using ports 9101, 8080, and 9090 with the following command:

```sh
lsof -ti:9101,8080,9090 | xargs kill -9
```

After running this command, you can safely restart the test exporter, ingestion server, and Prometheus. 