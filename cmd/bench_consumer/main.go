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
	partition := flag.Int("partition", -1, "Partition ID to consume from (default -1 for all partitions)")
	allPartitions := flag.Bool("all-partitions", true, "Consume from all partitions in parallel (concurrency mapped to partitions)")
	count := flag.Int64("count", 100000, "Total records to consume")
	maxFetchBytes := flag.Int("max-fetch-bytes", 1048576, "Maximum bytes to fetch per request (default 1MB)")
	concurrency := flag.Int("concurrency", 4, "Number of concurrent worker goroutines")
	offset := flag.Uint64("offset", 0, "Starting log offset to fetch from")
	pollInterval := flag.Duration("poll-interval", 10*time.Millisecond, "Duration to wait on empty partition fetch")
	idleTimeout := flag.Duration("idle-timeout", 30*time.Second, "Max duration to wait for new messages before halting")
	timeout := flag.Duration("timeout", 5*time.Second, "Request timeout limit per Fetch RPC")
	output := flag.String("output", "text", "Output format: text or json")
	outputFile := flag.String("output-file", "", "Output file path to save results")
	runLabel := flag.String("run-label", "", "Optional run label tag")
	warmup := flag.Bool("warmup", false, "Flag indicating if this is a warmup trial")
	runNumber := flag.Int("run-number", 1, "Run trial index number")
	flag.Parse()

	if *topicName == "" {
		fmt.Fprintln(os.Stderr, "Error: -topic must not be empty")
		os.Exit(1)
	}
	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -count must be greater than 0")
		os.Exit(1)
	}
	if *maxFetchBytes <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -max-fetch-bytes must be greater than 0")
		os.Exit(1)
	}
	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "Error: -concurrency must be greater than 0")
		os.Exit(1)
	}

	// Resolve partition logic
	targetPartition := -1
	if *partition != -1 {
		targetPartition = *partition
	} else if !*allPartitions {
		fmt.Fprintln(os.Stderr, "Error: Must specify either a valid -partition or enable -all-partitions")
		os.Exit(1)
	}

	// 1. Setup gRPC connection
	conn, err := grpc.NewClient(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to broker at %s: %v\n", *brokerAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	// 2. Instantiate and run consumer benchmark
	benchmarker := bench.NewConsumerBench(
		conn,
		*topicName,
		targetPartition,
		*count,
		*maxFetchBytes,
		*concurrency,
		*offset,
		*pollInterval,
		*idleTimeout,
		*timeout,
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
		fmt.Fprintf(os.Stderr, "Consumer benchmark execution failed: %v\n", err)
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
		fmt.Println("Consumer Benchmark Report")
		fmt.Println("========================================")
		fmt.Printf("Topic:            %s\n", result.Topic)
		fmt.Printf("Partition Mode:   %s\n", result.PartitionMode)
		fmt.Printf("Target:           %d\n", result.TargetRecords)
		fmt.Printf("Observed:         %d\n", result.ObservedRecords)
		fmt.Printf("Counted:          %d\n", result.CountedRecords)
		fmt.Printf("Overshoot:        %d\n", result.OvershootRecords)
		fmt.Printf("RPC Count:        %d\n", result.RPCCount)
		fmt.Printf("Max Fetch Bytes:  %d\n", result.MaxFetchBytes)
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
			txt := fmt.Sprintf("Topic: %s\nTarget: %d\nThroughput: %.2f msgs/sec\n", result.Topic, result.TargetRecords, result.MessagesPerSecond)
			_ = os.WriteFile(*outputFile, []byte(txt), 0644)
		}
	}
}
