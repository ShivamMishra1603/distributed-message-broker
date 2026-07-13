package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/bench"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	commit    = "unknown"
	dirty     = "false"
	buildTime = "unknown"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "Broker gRPC address")
	topicName := flag.String("topic", "bench-topic", "Topic name")
	msgSize := flag.Int("msg-size", 100, "Record message payload size in bytes")
	count := flag.Int64("count", 100000, "Total records to produce")
	duration := flag.Duration("duration", 0, "Run duration (e.g. 15s). If > 0, -count is ignored.")
	concurrency := flag.Int("concurrency", 4, "Number of concurrent workers")
	batchSize := flag.Int("batch-size", 100, "Number of records to batch per Produce request")
	partition := flag.Int("partition", -1, "Target partition ID (-1 for round-robin across partitions)")
	failFast := flag.Bool("fail-fast", true, "Halt run and exit nonzero immediately on first request failure")
	timeout := flag.Duration("timeout", 5*time.Second, "Request timeout limit per Produce RPC")
	output := flag.String("output", "text", "Output format: text or json")
	outputFile := flag.String("output-file", "", "Output file path to save results")
	runLabel := flag.String("run-label", "", "Optional run label tag")
	warmup := flag.Bool("warmup", false, "Flag indicating if this is a warmup trial")
	flushMode := flag.String("flush-mode", "sync", "Broker flush durability mode (sync or async)")
	runNumber := flag.Int("run-number", 1, "Run trial index number")
	flag.Parse()

	if *topicName == "" {
		fmt.Fprintln(os.Stderr, "Error: -topic must not be empty")
		os.Exit(1)
	}
	if *msgSize <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -msg-size must be greater than 0")
		os.Exit(1)
	}
	if *duration <= 0 && *count <= 0 {
		fmt.Fprintln(os.Stderr, "Error: either -count or -duration must be greater than 0")
		os.Exit(1)
	}
	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -concurrency must be greater than 0")
		os.Exit(1)
	}
	if *batchSize <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -batch-size must be greater than 0")
		os.Exit(1)
	}

	// 1. Setup gRPC connection
	conn, err := grpc.NewClient(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to broker at %s: %v\n", *brokerAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	// 2. Instantiate and run producer benchmark
	benchmarker := bench.NewProducerBench(
		conn,
		*topicName,
		*msgSize,
		*count,
		*duration,
		*concurrency,
		*batchSize,
		*partition,
		*failFast,
		*timeout,
		*flushMode,
		*runLabel,
		*runNumber,
		*warmup,
		commit,
		dirty == "true",
		buildTime,
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
		"1.0.0",
	)

	result, err := benchmarker.Run(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Producer benchmark execution failed: %v\n", err)
		os.Exit(1)
	}

	// 3. Format and output results
	if *output == "json" {
		data, jsonErr := json.MarshalIndent(result, "", "  ")
		if jsonErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal JSON result: %v\n", jsonErr)
			os.Exit(1)
		}
		if *outputFile != "" {
			if writeErr := os.WriteFile(*outputFile, data, 0644); writeErr != nil {
				fmt.Fprintf(os.Stderr, "Failed to write results file: %v\n", writeErr)
				os.Exit(1)
			}
		} else {
			fmt.Println(string(data))
		}
	} else {
		// Output text report
		fmt.Println("========================================")
		fmt.Println("Producer Benchmark Report")
		fmt.Println("========================================")
		fmt.Printf("Topic:            %s\n", result.Topic)
		fmt.Printf("Partitions:       %d\n", result.Partitions)
		fmt.Printf("Partition Mode:   %s\n", result.PartitionMode)
		fmt.Printf("Flush Mode:       %s\n", result.FlushMode)
		fmt.Printf("Requested:        %d\n", result.RequestedRecords)
		fmt.Printf("Acknowledged:     %d\n", result.AcknowledgedRecords)
		fmt.Printf("Failed:           %d\n", result.FailedRecords)
		fmt.Printf("RPC Count:        %d\n", result.RPCCount)
		fmt.Printf("Record Size:      %d Bytes\n", result.RecordSizeBytes)
		fmt.Printf("Batch Size:       %d\n", result.BatchSize)
		fmt.Printf("Concurrency:      %d\n", result.Concurrency)
		fmt.Printf("Duration:         %.3fs\n", result.DurationSeconds)
		fmt.Printf("Throughput:       %.2f msgs/sec (%.2f MiB/sec)\n\n", result.MessagesPerSecond, result.PayloadMiBPerSecond)

		fmt.Println("Latency (RPC):")
		fmt.Printf("  Min:  %.2fms\n", result.Latency.MinMs)
		fmt.Printf("  Mean: %.2fms\n", result.Latency.MeanMs)
		fmt.Printf("  P50:  %.2fms\n", result.Latency.P50Ms)
		fmt.Printf("  P95:  %.2fms\n", result.Latency.P95Ms)
		fmt.Printf("  P99:  %.2fms\n", result.Latency.P99Ms)
		fmt.Printf("  Max:  %.2fms\n\n", result.Latency.MaxMs)

		if len(result.Errors) > 0 {
			fmt.Println("Errors / Cancellations:")
			for code, count := range result.Errors {
				fmt.Printf("  %s: %d\n", code, count)
			}
		} else {
			fmt.Println("Errors:           None")
		}
		fmt.Println("========================================")

		if *outputFile != "" {
			// Write text representation to file
			txt := fmt.Sprintf("Topic: %s\nPartitions: %d\nThroughput: %.2f msgs/sec\n", result.Topic, result.Partitions, result.MessagesPerSecond)
			_ = os.WriteFile(*outputFile, []byte(txt), 0644)
		}
	}

	// Exit non-zero if any failures occurred
	if result.FailedRecords > 0 {
		fmt.Fprintf(os.Stderr, "Run failed with %d records failing to acknowledge\n", result.FailedRecords)
		os.Exit(1)
	}
}
