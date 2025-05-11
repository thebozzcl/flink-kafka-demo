#!/bin/bash

# Default values
TOPIC_NAME="output-topic"
KAFKA_BROKER="localhost:9092"
MAX_MESSAGES=0  # 0 means read indefinitely

# Parse command line options
while getopts "t:b:n:h" opt; do
  case $opt in
    t) TOPIC_NAME="$OPTARG" ;;
    b) KAFKA_BROKER="$OPTARG" ;;
    n) MAX_MESSAGES="$OPTARG" ;;
    h)
      echo "Usage: $0 [-t topic_name] [-b broker_address] [-n max_messages]"
      echo "  -t  Topic name (default: output-topic)"
      echo "  -b  Kafka broker address (default: localhost:29092)"
      echo "  -n  Maximum number of messages to read (default: 0 = indefinitely)"
      exit 0
      ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

echo "Starting to consume messages from topic: $TOPIC_NAME"
echo "Press Ctrl+C to stop"

# Construct the command based on whether we want to limit the number of messages
if [ "$MAX_MESSAGES" -gt 0 ]; then
    kafka-console-consumer.sh \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$TOPIC_NAME" \
        --from-beginning \
        --max-messages "$MAX_MESSAGES"
else
    kafka-console-consumer.sh \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$TOPIC_NAME" \
        --from-beginning
fi
