#!/bin/bash

# Set up trap to handle CTRL+C
trap 'echo "Script interrupted. Exiting..."; exit 1' INT

# Check if required arguments are provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <input_file> <topic_name>"
    exit 1
fi

INPUT_FILE=$1
INPUT_TOPIC="input-topic"
KAFKA_BROKER="http://localhost:9092"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found."
    exit 1
fi

# Read file line by line and send to Kafka
while IFS= read -r line; do
    # Skip empty lines
    if [ -n "$line" ]; then
        echo "Sending: $line"
        echo "$line" | kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER --topic $INPUT_TOPIC

        # Check if the last command was successful
        if [ $? -ne 0 ]; then
            echo "Error sending message to Kafka. Exiting."
            exit 1
        fi

        # Optional: Add a small delay between messages
        sleep 0.1
    fi
done < "$INPUT_FILE"

echo "All messages sent to Kafka topic: $INPUT_TOPIC"
