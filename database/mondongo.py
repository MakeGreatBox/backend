import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime

# MQTT Broker Configuration
BROKER = "pi5"  # Public MQTT broker
PORT = 1883
TOPIC = ["machine/start", "machine/stop" , "machine/emergencyStop", "machine/consume"]

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
DB_NAME = "my_mongo"
COLLECTION_NAME = "messages"

# Connect to MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# Define the on_connect callback
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully!")
        # Subscribe to all topics with QoS 0
        topics_with_qos = [(topic, 0) for topic in TOPIC]  # Add QoS 0 to each topic
        client.subscribe(topics_with_qos)  # Pass the list of tuples
        print(f"Subscribed to topics: {TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")

# Define the on_message callback
def on_message(client, userdata, msg):
    message = msg.payload.decode()  # Decode the message
    topic = msg.topic
    timestamp = datetime.utcnow()  # Get the current timestamp in UTC

    # Prepare the document to insert into MongoDB
    document = {
        "topic": topic,
        "message": message,
        "timestamp": timestamp
    }

    # Insert into MongoDB
    result = collection.insert_one(document)
    print(f"Message saved to MongoDB with ID: {result.inserted_id}")

# Create MQTT Client
mqtt_client = mqtt.Client()

# Assign callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT Broker
mqtt_client.connect(BROKER, PORT, 60)

# Start the loop to process messages
mqtt_client.loop_forever()
