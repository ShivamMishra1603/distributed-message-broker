package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:50051", "Broker gRPC address")
	topicName := flag.String("topic", "", "Topic name (required)")
	partition := flag.Int("partition", -1, "Explicit partition (optional)")
	key := flag.String("key", "", "Record key (optional)")
	value := flag.String("value", "", "Record value (required)")
	flag.Parse()

	if *topicName == "" || *value == "" {
		fmt.Fprintln(os.Stderr, "Error: --topic and --value flags are required")
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

	adminClient := brokerpb.NewAdminServiceClient(conn)
	brokerClient := brokerpb.NewBrokerServiceClient(conn)

	// Resolve partition count to perform routing
	descResp, err := adminClient.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: *topicName})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to describe topic %q: %v\n", *topicName, err)
		os.Exit(1)
	}

	partitionCount := descResp.GetTopic().GetPartitionCount()
	if partitionCount == 0 {
		fmt.Fprintf(os.Stderr, "Topic %q has 0 partitions configured\n", *topicName)
		os.Exit(1)
	}

	var targetPartition uint32

	if *partition != -1 {
		if *partition < 0 || uint32(*partition) >= partitionCount {
			fmt.Fprintf(os.Stderr, "Error: explicit partition %d is out of range [0, %d)\n", *partition, partitionCount)
			os.Exit(1)
		}
		targetPartition = uint32(*partition)
	} else if *key != "" {
		h := fnv.New32a()
		h.Write([]byte(*key))
		targetPartition = h.Sum32() % partitionCount
		fmt.Printf("Routed key %q to partition %d using stable FNV-1a hash.\n", *key, targetPartition)
	} else {
		// Seed random partition selection
		targetPartition = uint32(rand.Intn(int(partitionCount)))
		fmt.Printf("No partition or key supplied. Randomly routed to partition %d.\n", targetPartition)
	}

	record := &brokerpb.Record{
		Key:       []byte(*key),
		Value:     []byte(*value),
		Timestamp: time.Now().UnixMilli(),
	}

	resp, err := brokerClient.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     *topicName,
		Partition: targetPartition,
		Records:   []*brokerpb.Record{record},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to produce record: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully produced record. BaseOffset: %d, LastOffset: %d, Partition: %d\n",
		resp.GetBaseOffset(), resp.GetLastOffset(), targetPartition)
}
