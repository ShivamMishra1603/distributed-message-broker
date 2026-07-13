#!/usr/bin/env bash
set -euo pipefail

# Resolving absolute path to the workspace root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$WORKSPACE_ROOT"

# Ensure binaries exist
echo "Building executables..."
commitSHA=$(git rev-parse HEAD | cut -c1-8)
dirtyState=$(git status --porcelain | grep -q . && echo "true" || echo "false")
buildTime=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

go build -ldflags "-X main.commit=${commitSHA} -X main.dirty=${dirtyState} -X main.buildTime=${buildTime}" -o ./bin/broker ./cmd/broker
go build -ldflags "-X main.commit=${commitSHA} -X main.dirty=${dirtyState} -X main.buildTime=${buildTime}" -o ./bin/bench_producer ./cmd/bench_producer
go build -ldflags "-X main.commit=${commitSHA} -X main.dirty=${dirtyState} -X main.buildTime=${buildTime}" -o ./bin/bench_consumer ./cmd/bench_consumer
go build -ldflags "-X main.commit=${commitSHA} -X main.dirty=${dirtyState} -X main.buildTime=${buildTime}" -o ./bin/admin ./cmd/admin

# Setup Unique Results Directory
DATE_STR=$(date +'%Y-%m-%dT%H%M%S')
OS_NAME=$(uname | tr '[:upper:]' '[:lower:]')
ARCH_NAME=$(uname -m | tr '[:upper:]' '[:lower:]')
DIRTY_SUFFIX=""
if [ "$dirtyState" = "true" ]; then
    DIRTY_SUFFIX="-dirty"
fi
RESULT_DIR="benchmarks/results/${DATE_STR}-${OS_NAME}-${ARCH_NAME}-${commitSHA}${DIRTY_SUFFIX}"
mkdir -p "$RESULT_DIR"

echo "Results will be saved to: $RESULT_DIR"

# Global Process PIDs to clean on exit
BROKER_PID=""

cleanup() {
    echo "Performing cleanup..."
    if [ -n "$BROKER_PID" ]; then
        echo "Stopping broker process (PID: $BROKER_PID)..."
        kill "$BROKER_PID" || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Helper function to start broker and wait for readiness
start_broker() {
    local config_file="$1"
    echo "Starting broker with config: $config_file"
    rm -rf ./benchmarks/data
    mkdir -p ./benchmarks/data

    ./bin/broker -config "$config_file" > "$RESULT_DIR/broker.log" 2>&1 &
    BROKER_PID=$!
    echo "Broker launched with PID: $BROKER_PID"

    # Poll readiness endpoint
    echo "Waiting for broker to be ready..."
    local ready=0
    for _ in $(seq 1 60); do
        if curl -fsS http://localhost:9093/readyz >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done

    if [ "$ready" -ne 1 ]; then
        echo "Error: Broker failed to become ready within 60s."
        exit 1
    fi
    echo "Broker is ready."
}

# Helper function to stop broker
stop_broker() {
    if [ -n "$BROKER_PID" ]; then
        echo "Stopping broker..."
        kill "$BROKER_PID" || true
        wait "$BROKER_PID" 2>/dev/null || true
        BROKER_PID=""
    fi
}

# Collect Machine Metadata
echo "Collecting machine metadata..."
METADATA_FILE="$RESULT_DIR/metadata.json"
cat <<EOF > "$METADATA_FILE"
{
  "date": "$(date)",
  "commit": "$commitSHA",
  "dirty": $dirtyState,
  "os": "$(uname -s)",
  "arch": "$(uname -m)",
  "go_version": "$(go version)",
  "cpu": "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep "Model name" | cut -d':' -f2 | xargs || echo "Unknown CPU")",
  "memory": "$(sysctl -n hw.memsize 2>/dev/null || free -b 2>/dev/null | grep Mem | awk '{print $2}' || echo "Unknown RAM")"
}
EOF

# ==============================================================================
# Warmup and Scenarios
# ==============================================================================
start_broker "benchmarks/configs/sync.yaml"

echo "Creating benchmark topic..."
./bin/admin --broker localhost:9092 create-topic bench-topic --partitions 4 || true

# Run Warmup
echo "Running warmup (excluded from statistics)..."
./bin/bench_producer -broker localhost:9092 -topic bench-topic -count 10000 -concurrency 2 -batch-size 10 -warmup > /dev/null
./bin/bench_consumer -broker localhost:9092 -topic bench-topic -count 10000 -concurrency 2 -warmup > /dev/null

# ------------------------------------------------------------------------------
# SCENARIO MATRIX BASELINES (Runs 1-3 per scenario)
# ------------------------------------------------------------------------------

# S1: Flush Mode (sync vs async)
for mode in sync async; do
    stop_broker
    start_broker "benchmarks/configs/${mode}.yaml"
    ./bin/admin --broker localhost:9092 create-topic bench-topic --partitions 4 || true

    for run in 1 2 3; do
        echo "Running S1 Flush Mode: ${mode}, Trial: ${run}"
        ./bin/bench_producer -broker localhost:9092 -topic bench-topic -duration 10s -count 0 -concurrency 4 -batch-size 100 -flush-mode "$mode" -run-number "$run" -output json -output-file "$RESULT_DIR/s1_prod_${mode}_trial_${run}.json"
        
        # Prepare dataset info in results, then run consumer in duration replay mode
        ./bin/bench_consumer -broker localhost:9092 -topic bench-topic -duration 10s -count 0 -concurrency 4 -run-number "$run" -output json -output-file "$RESULT_DIR/s1_cons_${mode}_trial_${run}.json"
    done
done

# Rest of tests run in Sync Mode
stop_broker
start_broker "benchmarks/configs/sync.yaml"

# S2: Batching Impact
for b_size in 1 10 100 500; do
    ./bin/admin --broker localhost:9092 create-topic "bench-s2-${b_size}" --partitions 4 || true
    
    for run in 1 2 3; do
        echo "Running S2 Batching: ${b_size}, Trial: ${run}"
        ./bin/bench_producer -broker localhost:9092 -topic "bench-s2-${b_size}" -duration 10s -count 0 -concurrency 4 -batch-size "$b_size" -run-number "$run" -output json -output-file "$RESULT_DIR/s2_prod_batch_${b_size}_trial_${run}.json"
    done
done

# S3: Record Payload Size Impact
for m_size in 100 1024 10240; do
    ./bin/admin --broker localhost:9092 create-topic "bench-s3-${m_size}" --partitions 4 || true
    for run in 1 2 3; do
        echo "Running S3 Message Size: ${m_size}B, Trial: ${run}"
        ./bin/bench_producer -broker localhost:9092 -topic "bench-s3-${m_size}" -msg-size "$m_size" -duration 10s -count 0 -concurrency 4 -batch-size 100 -run-number "$run" -output json -output-file "$RESULT_DIR/s3_prod_msgsize_${m_size}_trial_${run}.json"
    done
done

# S4: Concurrency and Partition Scaling
for conc in 1 4 16 32; do
    # Multi partition
    ./bin/admin --broker localhost:9092 create-topic "bench-s4-multi-${conc}" --partitions 4 || true
    # Single partition
    ./bin/admin --broker localhost:9092 create-topic "bench-s4-single-${conc}" --partitions 1 || true

    for run in 1 2 3; do
        echo "Running S4 Concurrency: ${conc} (multi-partition), Trial: ${run}"
        ./bin/bench_producer -broker localhost:9092 -topic "bench-s4-multi-${conc}" -duration 10s -count 0 -concurrency "$conc" -batch-size 100 -run-number "$run" -output json -output-file "$RESULT_DIR/s4_prod_multi_conc_${conc}_trial_${run}.json"

        echo "Running S4 Concurrency: ${conc} (single-partition), Trial: ${run}"
        ./bin/bench_producer -broker localhost:9092 -topic "bench-s4-single-${conc}" -partition 0 -duration 10s -count 0 -concurrency "$conc" -batch-size 100 -run-number "$run" -output json -output-file "$RESULT_DIR/s4_prod_single_conc_${conc}_trial_${run}.json"
    done
done

# S5: Fetch Size Throughput
./bin/admin --broker localhost:9092 create-topic "bench-s5" --partitions 4 || true
# Pre-produce dataset
./bin/bench_producer -broker localhost:9092 -topic "bench-s5" -count 1000000 -concurrency 4 -batch-size 100 >/dev/null

for f_size in 65536 1048576 5242880; do
    for run in 1 2 3; do
        echo "Running S5 Fetch size: ${f_size} bytes, Trial: ${run}"
        ./bin/bench_consumer -broker localhost:9092 -topic "bench-s5" -duration 10s -count 0 -concurrency 4 -max-fetch-bytes "$f_size" -run-number "$run" -output json -output-file "$RESULT_DIR/s5_cons_fetchsize_${f_size}_trial_${run}.json"
    done
done

# ==============================================================================
# S6: Restart Recovery and Index Reconstruction
# ==============================================================================
stop_broker
echo "Preparing clean database copy for recovery trials..."
rm -rf ./benchmarks/data ./benchmarks/data_prep
start_broker "benchmarks/configs/sync.yaml"
./bin/admin --broker localhost:9092 create-topic "bench-s6" --partitions 1 || true

# Pre-produce data
./bin/bench_producer -broker localhost:9092 -topic "bench-s6" -count 100000 -concurrency 4 -batch-size 100 > /dev/null
stop_broker
cp -R ./benchmarks/data ./benchmarks/data_prep

valid_times=()
rebuild_times=()

get_median() {
    local arr=($(for val in "$@"; do echo "$val"; done | sort -n))
    local len=${#arr[@]}
    if [ $((len % 2)) -eq 1 ]; then
        echo "${arr[$((len / 2))]}"
    else
        local mid1="${arr[$((len / 2 - 1))]}"
        local mid2="${arr[$((len / 2))]}"
        echo "$(( (mid1 + mid2) / 2 ))"
    fi
}

# Valid indexes trials
for run in 1 2 3 4 5; do
    echo "Running S6 Valid Indexes recovery, Trial: ${run}"
    rm -rf ./benchmarks/data
    cp -R ./benchmarks/data_prep ./benchmarks/data
    
    recStart=$(date +%s%N)
    ./bin/broker -config "benchmarks/configs/sync.yaml" > /dev/null 2>&1 &
    BROKER_PID=$!
    
    ready=0
    for _ in $(seq 1 6000); do
        if curl -fsS http://localhost:9093/readyz >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 0.01
    done
    recDuration=$(( ($(date +%s%N) - recStart) / 1000000 ))
    
    if [ "$ready" -ne 1 ]; then
        echo "Error: Broker failed to start in valid index trial ${run}" >&2
        exit 1
    fi
    echo "Trial ${run} took: ${recDuration}ms"
    valid_times+=($recDuration)
    stop_broker
done

# Rebuild indexes trials
for run in 1 2 3 4 5; do
    echo "Running S6 Rebuild Indexes recovery, Trial: ${run}"
    rm -rf ./benchmarks/data
    cp -R ./benchmarks/data_prep ./benchmarks/data
    find ./benchmarks/data -name "*.index" -type f -delete
    
    recStart=$(date +%s%N)
    ./bin/broker -config "benchmarks/configs/sync.yaml" > /dev/null 2>&1 &
    BROKER_PID=$!
    
    ready=0
    for _ in $(seq 1 6000); do
        if curl -fsS http://localhost:9093/readyz >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 0.01
    done
    rebuildDuration=$(( ($(date +%s%N) - recStart) / 1000000 ))
    
    if [ "$ready" -ne 1 ]; then
        echo "Error: Broker failed to start in rebuild index trial ${run}" >&2
        exit 1
    fi
    
    # Verify indexes exist
    num_indexes=$(find ./benchmarks/data -name "*.index" | wc -l)
    if [ "$num_indexes" -eq 0 ]; then
        echo "Error: No indexes rebuilt in trial ${run}" >&2
        exit 1
    fi
    
    # Verify a known fetch succeeds
    ./bin/bench_consumer -broker localhost:9092 -topic "bench-s6" -count 10 -concurrency 1 >/dev/null
    
    echo "Trial ${run} took: ${rebuildDuration}ms (rebuilt $num_indexes indexes)"
    rebuild_times+=($rebuildDuration)
    stop_broker
done

valid_median=$(get_median "${valid_times[@]}")
rebuild_median=$(get_median "${rebuild_times[@]}")

echo "Valid Indexes median: ${valid_median}ms"
echo "Index Reconstruction median: ${rebuild_median}ms"

# Write JSON recovery outputs
echo "{\"valid_trials\": [$(echo "${valid_times[@]}" | tr ' ' ',')], \"median_ms\": ${valid_median}}" > "$RESULT_DIR/s6_recovery_valid.json"
echo "{\"rebuild_trials\": [$(echo "${rebuild_times[@]}" | tr ' ' ',')], \"median_ms\": ${rebuild_median}}" > "$RESULT_DIR/s6_recovery_rebuild.json"

# Clean prep files
rm -rf ./benchmarks/data_prep

# ==============================================================================
# S7: Retention Cleanup
# ==============================================================================
echo "Running S7 retention benchmark..."
start_broker "benchmarks/configs/retention.yaml"
./bin/admin --broker localhost:9092 create-topic "bench-s7" --partitions 1 || true

# Produce 50,000 records of 100 bytes (approx 5.5MB of data)
./bin/bench_producer -broker localhost:9092 -topic "bench-s7" -count 50000 -concurrency 4 -batch-size 100 >/dev/null

# Check files before retention triggers
files_before=$(ls ./benchmarks/data/topics/bench-s7/partition-0)
num_before=$(echo "$files_before" | wc -l)
echo "Files before retention: $num_before"

retStart=$(date +%s%N)

# Wait up to 10 seconds for retention check to fire and delete files
deleted=0
for _ in $(seq 1 100); do
    files_now=$(ls ./benchmarks/data/topics/bench-s7/partition-0 2>/dev/null || true)
    num_now=$(echo "$files_now" | wc -l)
    if [ "$num_before" -gt "$num_now" ]; then
        deleted=1
        break
    fi
    sleep 0.1
done

retDuration=$(( ($(date +%s%N) - retStart) / 1000000 ))

if [ "$deleted" -ne 1 ]; then
    echo "Error: Retention cleanup failed to delete old segments within 10s."
    exit 1
fi

echo "Retention detection and cleanup completed in ${retDuration}ms"

# Perform retention assertions
files_after=$(ls ./benchmarks/data/topics/bench-s7/partition-0)
active_segment=$(echo "$files_after" | sort | tail -n 1)
echo "Active segment after retention: $active_segment"

if [ -z "$active_segment" ]; then
    echo "Error: Active segment was deleted!"
    exit 1
fi

# Verify orphan check (no indexes without matching log file)
for idx in ./benchmarks/data/topics/bench-s7/partition-0/*.index; do
    [ -e "$idx" ] || continue
    base="${idx%.index}"
    if [ ! -f "${base}.log" ]; then
        echo "Error: Orphaned index found: $idx"
        exit 1
    fi
done

# Write result
echo "{\"retention_deleted\": true, \"detection_and_cleanup_latency_ms\": ${retDuration}}" > "$RESULT_DIR/s7_retention.json"
stop_broker

# ==============================================================================
# DIAGNOSTIC PROFILING RUN
# ==============================================================================
echo "Starting diagnostic profiling run..."
stop_broker
start_broker "benchmarks/configs/diagnostic.yaml"
./bin/admin --broker localhost:9092 create-topic "bench-profile" --partitions 4 || true

# Spawn background workload to profile
./bin/bench_producer -broker localhost:9092 -topic "bench-profile" -count 200000 -concurrency 4 -batch-size 100 > /dev/null &
WORKLOAD_PID=$!

echo "Collecting pprof profiles during active load..."
sleep 2
curl -sS -o "$RESULT_DIR/cpu.pprof" "http://localhost:9093/debug/pprof/profile?seconds=5" || true
curl -sS -o "$RESULT_DIR/heap.pprof" "http://localhost:9093/debug/pprof/heap" || true
curl -sS -o "$RESULT_DIR/allocs.pprof" "http://localhost:9093/debug/pprof/allocs" || true
curl -sS -o "$RESULT_DIR/goroutine.pprof" "http://localhost:9093/debug/pprof/goroutine" || true
curl -sS -o "$RESULT_DIR/mutex.pprof" "http://localhost:9093/debug/pprof/mutex" || true
curl -sS -o "$RESULT_DIR/block.pprof" "http://localhost:9093/debug/pprof/block" || true

# Wait for workload to finish
wait "$WORKLOAD_PID" 2>/dev/null || true

# Generate top summaries
if command -v go >/dev/null 2>&1; then
    echo "Generating text summaries from profiles..."
    go tool pprof -top ./bin/broker "$RESULT_DIR/cpu.pprof" > "$RESULT_DIR/cpu-top.txt" 2>/dev/null || true
    go tool pprof -top ./bin/broker "$RESULT_DIR/heap.pprof" > "$RESULT_DIR/heap-top.txt" 2>/dev/null || true
fi

# Collect OS resources if available
if command -v top >/dev/null 2>&1; then
    if [ "$OS_NAME" = "darwin" ]; then
        top -l 1 -n 5 > "$RESULT_DIR/top.txt" 2>/dev/null || true
    else
        top -b -n 1 | head -n 30 > "$RESULT_DIR/top.txt" 2>/dev/null || true
    fi
fi

if command -v iostat >/dev/null 2>&1; then
    iostat -x 1 3 > "$RESULT_DIR/iostat.txt" 2>/dev/null || true
fi

stop_broker
echo "All benchmarks completed successfully."
