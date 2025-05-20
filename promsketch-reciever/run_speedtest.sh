#!/bin/bash

distributions=("uniform" "normal" "exponential")
sizes=(1000 10000 100000 1000000 10000000)

echo "Starting speedtests..."
echo "Distribution,Size,IngestionTimeMs,QueryTimeMs,MemorySavingsPercent" > results.csv

for dist in "${distributions[@]}"; do
    for size in "${sizes[@]}"; do
        echo "Testing $dist with size $size"
        result=$(curl -s "http://localhost:8080/speedtest?metric=test_$dist&size=$size")
        ingestion_time=$(echo "$result" | grep -o '"ingestion_time_ms":[0-9.]*' | cut -d':' -f2)
        query_time=$(echo "$result" | grep -o '"query_time_ms":[0-9.]*' | cut -d':' -f2)
        memory_savings=$(echo "$result" | grep -o '"memory_savings_percent":[0-9.]*' | cut -d':' -f2)
        echo "$dist,$size,$ingestion_time,$query_time,$memory_savings" >> results.csv
        echo "----------------------------------------"
        sleep 1  # Small delay between tests
    done
done

echo "Results saved to results.csv" 