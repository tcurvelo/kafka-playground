package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

var TOPIC = "example-topic"
var BROKER = "localhost:9092"

type Key struct {
	User   string
	Urgent bool
}

type Value struct {
	Message string
}

func makeMessage(key Key, value Value) kafka.Message {
	keyBytes, err := json.Marshal(key)
	if err != nil {
		panic("could not marshal key: " + err.Error())
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		panic("could not marshal key: " + err.Error())
	}
	return kafka.Message{
		Key:   keyBytes,
		Value: valueBytes,
	}
}

type BucketBalancer struct {
	WithinBucketBalancer kafka.CRC32Balancer
	SplitAt              float64 // e.g., 0.8 for top 20%
}

func (u BucketBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	pivot := int(float64(len(partitions)) * u.SplitAt)

	// if Urgent, assign to "priority" bucket (aka. top partitions)
	// using the crc balancer within the bucket
	decodedKey := Key{}
	err := json.Unmarshal(msg.Key, &decodedKey)
	if err == nil && decodedKey.Urgent {
		topPartitions := partitions[pivot:]
		partition := u.WithinBucketBalancer.Balance(msg, topPartitions...)

		fmt.Printf("Assigning urgent message to partition %v (candidates: %v)\n", partition, topPartitions)
		return partition
	}

	// fallback, use the hash-based partitioning within bottom bucket
	bottomPartitions := partitions[:pivot]
	partition := u.WithinBucketBalancer.Balance(msg, bottomPartitions...)

	fmt.Printf("Assigning non-urgent message to partition %v (candidates: %v)\n", partition, bottomPartitions)
	return partition
}

func main() {
	naiveWriter := &kafka.Writer{
		Addr:     kafka.TCP(BROKER),
		Topic:    TOPIC,
		Balancer: kafka.CRC32Balancer{},
	}
	defer naiveWriter.Close()

	priorityWriter := &kafka.Writer{
		Addr:     kafka.TCP(BROKER),
		Topic:    TOPIC,
		Balancer: BucketBalancer{SplitAt: 0.8},
	}

	// defer priorityWriter.Close()
	messages2naive := []kafka.Message{
		makeMessage(Key{User: "user-123", Urgent: false}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-123", Urgent: true}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-456", Urgent: false}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-456", Urgent: true}, Value{Message: "hello"}),
	}
	messages2priority := []kafka.Message{
		makeMessage(Key{User: "user-123", Urgent: false}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-123", Urgent: true}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-456", Urgent: false}, Value{Message: "hello"}),
		makeMessage(Key{User: "user-456", Urgent: true}, Value{Message: "hello"}),
	}
	ctx := context.Background()

	// produce messages using the naive writer
	err := naiveWriter.WriteMessages(ctx, messages2naive...)
	if err != nil {
		panic("could not write message " + err.Error())
	}

	// produce messages using the priority writer
	err = priorityWriter.WriteMessages(ctx, messages2priority...)
	if err != nil {
		panic("could not write message " + err.Error())
	}
}
