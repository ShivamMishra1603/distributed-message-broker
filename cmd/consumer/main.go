package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:50051", "Broker gRPC address")
	topicName := flag.String("topic", "", "Topic name (required)")
	partition := flag.Int("partition", -1, "Partition ID (required)")
	offset := flag.Uint64("offset", 0, "Starting offset to fetch from")
	flag.Parse()

	if *topicName == "" || *partition == -1 {
		fmt.Fprintln(os.Stderr, "Error: --topic and --partition flags are required")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := brokerpb.NewBrokerServiceClient(conn)

	// Fetch up to 1 MB of records at a time
	resp, err := client.Fetch(ctx, &brokerpb.FetchRequest{
		Topic:     *topicName,
		Partition: uint32(*partition),
		Offset:    *offset,
		MaxBytes:  1024 * 1024,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch records: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Fetched %d records. EarliestOffset: %d, LogEndOffset: %d\n",
		len(resp.GetRecords()), resp.GetEarliestOffset(), resp.GetLogEndOffset())

	for _, r := range resp.GetRecords() {
		fmt.Printf("----------------------------------------\n")
		fmt.Printf("Offset:    %d\n", r.GetOffset())
		fmt.Printf("Timestamp: %s\n", time.UnixMilli(r.GetTimestamp()).UTC().Format(time.RFC3339))
		fmt.Printf("Key:       %s\n", string(r.GetKey()))
		fmt.Printf("Value:     %s\n", string(r.GetValue()))
		if len(r.GetHeaders()) > 0 {
			fmt.Printf("Headers:\n")
			for _, h := range r.GetHeaders() {
				fmt.Printf("  %s: %s\n", h.GetKey(), string(h.GetValue()))
			}
		}
	}
}
