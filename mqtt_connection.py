from paho.mqtt import client as mqtt
from azure_connection import IoTDevice
from mongo_connection import MondongoDB
import json

class Mqtt:
    def __init__(self, topics,mondongo:MondongoDB, device:IoTDevice, telemetry):
        self.mondongo = mondongo
        self.device = device
        self.telemetry = telemetry
        self.client = mqtt.Client()
        self.client.connect("pi5", 1883)
        self.client.subscribe(topics)
        self.client.loop_start()
        self.client.on_message = self.on_message

    def on_message(self, client:mqtt.Client, userdata, msg:mqtt.MQTTMessage):
        topic = msg.topic
        data = msg.payload.decode()
        if topic == "machine/boxes":
            json_data = json.loads(data)
            self.telemetry["count"]=json_data["total_boxes"]
            client.publish("machine/boxcount",json_data["total_boxes"])
        elif topic=="machine/machineConsume":
            self.telemetry["energy"] = float(data)
            self.mondongo.telemetry.insert_many([self.device.get_json_telemetry(self.telemetry["speed"],self.telemetry["count"],self.telemetry["energy"])])
            self.device.send_telemetry(self.telemetry["speed"],self.telemetry["count"],self.telemetry["energy"])

    def publish(self, topic, payload, qos=0, retain=False):
        self.client.publish(topic, payload, qos, retain)