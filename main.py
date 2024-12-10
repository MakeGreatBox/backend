from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import json
from azure.iot.device import IoTHubDeviceClient, Message
from time import sleep

import paho.mqtt.client as mqtt
from pymongo import MongoClient
class IoTDevice:
    CONNECTION_STRING = "HostName=ra-develop-bobstconnect-01.azure-devices.net;DeviceId=LAUZHACKPI5;SharedAccessKey=cRbJIBv9IJEqd0W60+ogh0+ya+jTLVc34AIoTL+GEEY="
    MACHINE_ID = "lauzhack-pi5"
    DATA_IP = "10.0.4.95:80"

    def __init__(self):
        self.create_device_client()
        self.machine_id = self.MACHINE_ID
        self.PROC_ID = 1

    def create_device_client(self):
        try:
            self.client = IoTHubDeviceClient.create_from_connection_string(self.CONNECTION_STRING)
            self.client.connect()
            print("Successfully connected to IoT Hub!")
        except Exception as e:
            print(f"Error connecting to IoT Hub: {e}")
    
    def get_json_telemetry(self, speed, count, energy):
        return {
            "telemetry" : {
                "machineid": self.MACHINE_ID,
                "datasource": self.DATA_IP,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "machinespeed" : speed,
                "totaloutputunitcount" : count,
                "totalworkingenergy": energy
            }
        }
    def send_telemetry(self, speed, count, energy):
        telemetry_data = {
            "telemetry" : {
                "machineid": self.MACHINE_ID,
                "datasource": self.DATA_IP,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "machinespeed" : speed,
                "totaloutputunitcount" : count,
                "totalworkingenergy": energy
            }
        }
    
        self.__send_message(telemetry_data,"Telemetry")
    
    def get_machine_event(self, event_type, job_id, total_output_unit_count, machine_speed):

        return {
            "type": event_type,
            "equipmentId": self.machine_id,
            "jobId": job_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "machineSpeed": machine_speed,
            "totaloutputunitcount": total_output_unit_count,
        }

    def send_machine_event(self, event_type, job_id, total_output_unit_count, machine_speed):

        machine_event = {
            "type": event_type,
            "equipmentId": self.machine_id,
            "jobId": job_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "machineSpeed": machine_speed,
            "totaloutputunitcount": total_output_unit_count,
        }

        self.__send_message([machine_event], "MachineEvent")

    def increase_id(self):
        self.PROC_ID = self.PROC_ID+1
        return self.PROC_ID
    def __send_message(self, payload, event_type):
        message = Message(json.dumps(payload))
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.custom_properties["messageType"] = event_type
        self.client.send_message(message)

    def close(self):
        self.client.disconnect()


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "my_mongo"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
event_collection = db["machine_events"]
telemetry_collection = db["telemetry"]

telemetry_data = {"speed":0.20,"count":0,"energy":0}
device = IoTDevice()

def get_time_json():
    return json.dumps({"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")})

def on_message(client, userdata, msg):
    topic = msg.topic
    data = msg.payload.decode()
    if topic == "machine/boxes":
        json_data = json.loads(data)
        telemetry_data["count"]=json_data["total_boxes"]
        client.publish("machine/boxcount",json_data["total_boxes"])
    elif topic=="machine/machineConsume":
        telemetry_data["energy"] = float(data)
        telemetry_collection.insert_many([device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])])
        device.send_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])
        

    
def on_connect(client, userdata, flags, rc):
    client.subscribe([("machine/machineConsume", 0), ("machine/start", 0), ("machine/velocity",0), ("machine/boxes",0), ("machine/stop",0)])

client = mqtt.Client()
client.connect("pi5", 1883)
client.loop_start()
client.on_connect = on_connect
client.on_message = on_message

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify allowed origins here
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
)
class State(BaseModel):
    state: bool
    
@app.post("/buttonState/")
def create_item(state:State):
    if state.state:
        event_collection.insert_many([device.get_machine_event("startProducing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])])
        device.send_machine_event("startProducing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])
        device.increase_id()
        client.publish("machine/start",True)
    else:
        event_collection.insert_many([device.get_machine_event("stopProducing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])])
        device.send_machine_event("stopProducing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])
        device.increase_id()
        client.publish("machine/stop",True)

@app.post("/speed/{value}/")
def get_data(value:int):
    mapper = {1:(0.15,0.2),2:(0.23,0.25), 3:(0.25,0.27)}

    speed_azure, speed_mqtt = mapper[value]
    client.publish("machine/velocity",speed_mqtt)
    telemetry_data["speed"]=speed_azure
    telemetry_collection.insert_many([device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])])
    device.send_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])

@app.get("/cosa/cosa/")
def get_story():
    stats = event_collection.find({"type":"stopProducing"})
    for i in stats:
        print(i["timestamp"])
        next_document = event_collection.find({"timestamp": {"$gt": i["timestamp"]}}).sort("timestamp", 1).limit(1)
        time = next_document["timestamp"]-i["timestamp"]