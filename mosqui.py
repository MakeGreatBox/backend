
import time
from datetime import datetime,timezone
import random
import paho.mqtt.client as mqtt
import json

def get_time_json():
    return json.dumps({"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")})

if __name__ == '__main__':
    broker = "localhost"
    port = 1883
    topic = "test/sensor"
    mqtt_topics = ["machine/start", "machine/stop" , "machine/emergencyStop", "machine/reset", "boxes/count", "machine/settings", "machine/velocity", "machine/pause", "machine/temperature"]
    client = mqtt.Client()
    client.connect(broker, port)
    client.loop_start()

    
    while (True):
        payload_json = json.dumps({"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")})

        client.publish("machine/start", get_time_json())
        time.sleep(1)
        client.publish("machine/stop", get_time_json())
        time.sleep(1)