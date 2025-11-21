# Kafka Avro System - Big Data Assignment

<br>

This project implements a **Kafka-based event processing system** using **Apache Kafka** , **Avro serialization** , and **Java**.

It demonstrates:
- Kafka Producer
- Kafka Consumer
- Avro serialization/deserialization
- Real-time aggregation (running average)
- Retry logic for temporary failures
- Dead Letter Queue (DLQ) for permanently failed messages
<br>

## 🚀 Technologies Used

- Java 17
- Apache Kafka
- Apache Avro
- Maven
- Docker & Docker Compose
- Kafka Clients Library
<br>

## 📁 Project Structure

```
src/
 └── main/
      └── java/com/example/
           ├── OrderProducer.java
           ├── OrderConsumer.java
           ├── OrderAggregator.java
           ├── OrderDLQConsumer.java
           ├── AvroUtils.java
           └── Order.java  (generated from schema)
      └── resources/
           └── order.avsc
docker-compose.yml
pom.xml
README.md
```
<br>

## 🐳 Starting Kafka with Docker

Run

``` 
docker-compose up -d

```

Check Kafka is running

```
docker ps

```
<br>

## ▶️ Running the Project
<br>

### 1️⃣ Run Producer

Sends 10 messages + one BAD message:

```
mvn exec:java -Dexec.mainClass="com.example.OrderProducer"

```
<br>

### 2️⃣ Run Consumer (with retry logic)

The consumer retries BAD messages 3 times, then sends to DLQ.

```
mvn exec:java -Dexec.mainClass="com.example.OrderConsumer"

```
<br>

### 3️⃣ Run Real-time Aggregator

Calculates running average of all prices.

```
mvn exec:java -Dexec.mainClass="com.example.OrderAggregator"

```
<br>

### 4️⃣ Run Dead Letter Queue (DLQ) Consumer

Reads failed messages from orders-dlq topic.

```
mvn exec:java -Dexec.mainClass="com.example.OrderDLQConsumer"

```

