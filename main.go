package main

import (
	"log"
	"math/rand"
	"time"

	kafka "github.com/IBM/sarama"
)

// a
func main() {
	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5

	producer, err := kafka.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create sync producer: %s", err)
	}
	consumer, err := kafka.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()
	defer producer.Close()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			message := generateRandomString(10)
			msg := &kafka.ProducerMessage{
				Topic: "test-topic",
				Key:   nil,
				Value: kafka.StringEncoder(message),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Fatalf("Failed to send message: %s", err)
			}
			log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, kafka.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %s", err)
	}
	defer partitionConsumer.Close()
	go func() {
		for msg := range partitionConsumer.Messages() {
			log.Printf("Message: %s Partition:%d Offset:%d\n", msg.Value, msg.Partition, msg.Offset)
		}
	}()
}

func generateRandomString(i int) string {
	letters := []rune("abcdefghigklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Seed(time.Now().UnixNano())
	result := make([]rune, i)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}
