import logging
import os
import boto3
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
from decimal import Decimal

load_dotenv()  # take environment variables from .env.

# Check AWS credentials are set up
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

if not aws_access_key_id or not aws_secret_access_key:
    raise ValueError("AWS credentials are not set up.") # Raise an error if credentials are not set up

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get environment variables
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = os.getenv('TOPIC_NAME', 'activity_every_day')
group_id = os.getenv('GROUP_ID', 'activity_every_day-group')

# Create a consumer for the topic
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[bootstrap_servers],
    auto_offset_reset='earliest',  # Start reading from the beginning
    enable_auto_commit=True,
    group_id=group_id,  # Set a group ID for managing offsets
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def convert_floats_to_decimal(item):
    """Recursively convert all float values in a dictionary to Decimal."""
    if isinstance(item, list):
        return [convert_floats_to_decimal(i) for i in item]
    elif isinstance(item, dict):
        return {k: convert_floats_to_decimal(v) for k, v in item.items()}
    elif isinstance(item, float):
        return Decimal(str(item))
    return item


# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('group2__health_tracker_daily_activity')

# Read messages from the topic
for message in consumer:
    # logging.info(f"Received message: {value}")
    value = convert_floats_to_decimal(message.value)
    try:
        response = table.put_item(Item=value)
        print(f"Inserted item into DynamoDB: {response}")
    except Exception as e:
        print(f"Error inserting item into DynamoDB: {e}")
