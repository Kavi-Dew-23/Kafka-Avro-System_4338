package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) throws Exception {

        // -------------------------------
        // Kafka Consumer Configuration
        // -------------------------------
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("orders"));

        // -------------------------------
        // Kafka Producer for DLQ
        // -------------------------------
        Properties dlqProps = new Properties();
        dlqProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> dlqProducer = new KafkaProducer<>(dlqProps);

        System.out.println("Order Consumer with Retry Logic Started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> record : records) {

                byte[] data = record.value();
                String key = record.key();

                boolean processed = false;

                for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                    try {
                        // Deserialize
                        Order order = AvroUtils.deserialize(data);

                        // -------------------------------
                        // Simulated failure check
                        // Throw exception if product == "BAD"
                        // -------------------------------
                        if (order.getProduct().toString().equals("BAD")) {
                            throw new RuntimeException("Simulated temporary failure");
                        }

                        System.out.println("Processed Successfully: " + order);
                        processed = true;
                        break;

                    } catch (Exception e) {
                        System.out.println("Failed Attempt " + attempt + " for key=" + key);

                        if (attempt == MAX_RETRIES) {
                            System.out.println("Sending to DLQ after max retries: " + key);

                            // Send message to DLQ topic
                            ProducerRecord<String, byte[]> dlqRecord =
                                    new ProducerRecord<>("orders-dlq", key, data);

                            dlqProducer.send(dlqRecord);
                        }

                        Thread.sleep(500); // wait before retry
                    }
                }
            }
        }
    }
}