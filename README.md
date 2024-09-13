# Streaming Processing with PySpark, Kafka, and PostgreSQL
This project demonstrates real-time data processing using **Apache Kafka**, **Apache Spark**, and **PostgreSQL**. Simulate retail purchase events, stream them through Kafka, process them with Spark Streaming, and store the aggregated results in a PostgreSQL database.

## Project Overview
The main objective of this project is to build a streaming processing system that:
- Consumes simulated retail events from Kafka
- Processes the streaming data using PySpark (Spark Streaming)
- Writes the aggregated results into PostgreSQL

## Technologies Used
- **Python** (3.8+)
- **Apache Kafka** (for event streaming)
- **Apache Spark** (for streaming data processing)
- **PostgreSQL** (for storing processed data)
- **Docker** (for containerized environments)
- **Makefile** (for automating common tasks)

## How to Run

This project uses a Makefile to automate common tasks. Here are the commands:

```bash
make docker-build    # Builds the Docker images for the various services.
make postgres        # Starts the PostgreSQL service inside a Docker container.
make kafka           # Starts Kafka services such as the Kafka broker and Kafka UI for monitoring.
make spark           # Starts Apache Spark inside a Docker container.
make spark-produce   # Produces fake events to Kafka.
make spark-consume   # Runs Spark Streaming to process the Kafka events.


