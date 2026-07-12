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
	partitions := flag.Int("partitions", 0, "Number of partitions (required for create-topic)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	subcommand := args[0]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to broker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := brokerpb.NewAdminServiceClient(conn)

	switch subcommand {
	case "create-topic":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: create-topic <name> --partitions <N>")
			os.Exit(1)
		}
		topicName := args[1]
		if *partitions <= 0 {
			fmt.Fprintln(os.Stderr, "Error: --partitions flag must be greater than zero")
			os.Exit(1)
		}

		resp, err := client.CreateTopic(ctx, &brokerpb.CreateTopicRequest{
			Name:           topicName,
			PartitionCount: uint32(*partitions),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create topic: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Topic %q created successfully with %d partitions.\n", resp.GetTopic().GetName(), resp.GetTopic().GetPartitionCount())

	case "list-topics":
		resp, err := client.ListTopics(ctx, &brokerpb.ListTopicsRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to list topics: %v\n", err)
			os.Exit(1)
		}
		if len(resp.GetTopics()) == 0 {
			fmt.Println("No topics found.")
			return
		}
		fmt.Println("Topics:")
		for _, t := range resp.GetTopics() {
			fmt.Printf("  - %s (%d partitions)\n", t.GetName(), t.GetPartitionCount())
		}

	case "describe-topic":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: describe-topic <name>")
			os.Exit(1)
		}
		topicName := args[1]

		resp, err := client.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{
			Name: topicName,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to describe topic: %v\n", err)
			os.Exit(1)
		}
		t := resp.GetTopic()
		fmt.Printf("Topic: %s\n", t.GetName())
		fmt.Printf("Partitions: %d\n", t.GetPartitionCount())
		for _, p := range t.GetPartitions() {
			fmt.Printf("  Partition %d: earliest_offset=%d, log_end_offset=%d\n",
				p.GetPartitionId(), p.GetEarliestOffset(), p.GetLogEndOffset())
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand %q\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: admin [--broker <host:port>] <subcommand> [args]")
	fmt.Println("Subcommands:")
	fmt.Println("  create-topic <name> --partitions <N>   Create a new topic")
	fmt.Println("  list-topics                            List all topics")
	fmt.Println("  describe-topic <name>                  Describe a topic and its partitions")
}
