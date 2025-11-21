package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) throws Exception {

        // Kafka configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Create producer
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        // Produce 10 messages
        for (int i = 0; i < 10; i++) {
            Order order;

            if (i == 5) {
                order = new Order("id-" + i, "BAD", 99.9f);  // <-- BAD MESSAGE
            } else {
                order = new Order("id-" + i, "Item1", 99.9f);
            }

            byte[] avroBytes = AvroUtils.serialize(order);

            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>("orders", order.getOrderId().toString(), avroBytes);

            producer.send(record);
            System.out.println("Produced: " + order);
            Thread.sleep(1000);
        }

        producer.close();
    }
}