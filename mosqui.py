
import time
from datetime import datetime,timezone
import random
import paho.mqtt.client as mqtt
import json
from time import sleep
def get_time_json():
    return json.dumps({"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")})

if __name__ == '__main__':
    broker = "pi5"
    port = 1883
    topic = "test/sensor"
    mqtt_topics = ["machine/start", "machine/stop" , "machine/emergencyStop", "machine/reset", "boxes/count", "machine/settings", "machine/velocity", "machine/pause", "machine/temperature"]
    client = mqtt.Client()
    client.connect(broker, port)
    client.loop_start()
    box = 0
    while True:
        box +=1
        client.publish("machine/start", True)
        #client.publish("machine/velocity", 0.2) 
        #client.publish("machine/boxcount", box)
        sleep(1)
