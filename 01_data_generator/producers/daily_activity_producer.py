from kafka import KafkaProducer
import pandas as pd
import json
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

bootstrap_servers = "localhost:9092"
topic_name = "activity_every_day"

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],  # Ensure this matches your Kafka bootstrap servers
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# csv_filename
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_filename = os.path.join(current_dir, '../simulated_data', 'daily_activity.csv')

# Load the daily activity data
daily_activity_data = pd.read_csv(csv_filename)

# Send the daily activity data to the Kafka topic
def send_daily_activity_data():
    for _, row in daily_activity_data.iterrows():
        producer.send(topic_name, row.to_dict())
    producer.flush() # Ensure all messages are sent
    logging.info(f"Daily activity data sent to Kafka topic: {topic_name}")
    producer.close() # Close the connection

send_daily_activity_data()
