#!/bin/bash

# Wait until Kafka is ready
while ! nc -z localhost 9092; do
  echo "Waiting for Kafka to be ready..."
  sleep 1
done

declare -a topics=("activity_every_day" "calories_every_hour" "heart_rate_every_second")

# List existing topics
existing_topics=$(docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092)

# Loop through the desired topics and create them if they don't exist
for topic in "${topics[@]}"; do
    if [[ ! "$existing_topics" =~ "$topic" ]]; then
        echo "Creating topic: $topic"
        docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    else
        echo "Topic already exists: $topic"
    fi
done
