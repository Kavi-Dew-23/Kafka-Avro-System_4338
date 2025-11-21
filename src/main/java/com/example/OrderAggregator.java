package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderAggregator {

    public static void main(String[] args) throws Exception {

        // Kafka Configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-aggregator-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders"));

        double totalPrice = 0.0;
        int count = 0;

        System.out.println("Order Aggregator Started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> record : records) {
                // Deserialize Avro bytes back to Order object
                Order order = AvroUtils.deserialize(record.value());

                count++;
                totalPrice += order.getPrice();

                double average = totalPrice / count;

                System.out.println("Received Order: " + order);
                System.out.println("Running Average Price: " + average);
                System.out.println("----------------------------------------");
            }
        }
    }
}