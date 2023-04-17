/* The MIT License (MIT)
 *
 * Copyright (c) 2022-present David G. Simmons
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
 package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

/* Struct to hold the data from the MQTT message */
type env struct {
	Sensor string  `json:"sensor"`
	CO2    int32   `json:"co2"`
	Temp   float32 `json:"temp"`
	Humid  float32 `json:"humid"`
	Time   int32   `json:"time"`
}

/* Read the configuration file and return a kafka.ConfigMap */
func ReadConfig(configFile string) kafka.ConfigMap {
	kafkaConfig := make(map[string]kafka.ConfigValue)
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			kafkaConfig[parameter] = value
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}
	return kafkaConfig
}

/* Send the message to the specified kafka topic */
func sendToKafka(topic string, message string) error {
	configFile := "./properties"
	conf := ReadConfig(configFile)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return fmt.Errorf("error creating producer: %w", err)
	}
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(message),
	}, nil)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Value))
				}
			}
		}
	}()
	p.Flush(15 * 1000)
	p.Close()
	return nil
}
/* Read MQTT messages from the specified broker and topic and send them to the kafka broker */
func ReadMQTTMessages(brokerUri string, username string, password string, topic string, qos byte) error {
	// Create an MQTT client options object
	opts := MQTT.NewClientOptions()
	// Set the broker URI, username, and password
	opts.AddBroker(brokerUri)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

	// Create an MQTT client
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	// Subscribe to the specified topic with the specified QoS level
	if token := client.Subscribe(topic, qos, func(client MQTT.Client, message MQTT.Message) {
		envData := env{}
		json.Unmarshal(message.Payload(), &envData)
		envData.Time = int32(time.Now().UnixMilli())
		mess, _ := json.Marshal(envData)
		fmt.Printf("Received message: %s\n", mess)
		sendToKafka("co_2", string(mess))
	}); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	// Wait for messages to arrive
	for {
		select {}
	}
}

func main() {
	err := ReadMQTTMessages("tcp://davidgs.com:8883", "", "", "co2", 0)
	if err != nil {
		fmt.Printf("Error reading MQTT messages: %v\n", err)
	}
}
