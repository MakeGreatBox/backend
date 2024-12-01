from datetime import datetime, timezone
import json
from azure.iot.device import IoTHubDeviceClient, Message
from time import sleep


class IoTDevice:
    CONNECTION_STRING = "HostName=ra-develop-bobstconnect-01.azure-devices.net;DeviceId=LAUZHACKPI5;SharedAccessKey=cRbJIBv9IJEqd0W60+ogh0+ya+jTLVc34AIoTL+GEEY="
    MACHINE_ID = "lauzhack-pi5"
    DATA_IP = "10.0.4.95:80"

    def __init__(self):
        self.create_device_client()
        self.machine_id = self.MACHINE_ID

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


    def __send_message(self, payload, event_type):
        message = Message(json.dumps(payload))
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.custom_properties["messageType"] = event_type
        self.client.send_message(message)

    def close(self):
        self.client.disconnect()


import paho.mqtt.client as mqtt
import json
from time import sleep

from pymongo import MongoClient
from datetime import datetime


# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
DB_NAME = "my_mongo"

# Connect to MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
event_collection = db["machine_events"]
telemetry_collection = db["telemetry"]
telemetry_data = {"speed":0.20,"count":0,"energy":0}
device = IoTDevice()
randid=1
def get_time_json():
    return json.dumps({"timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")})
def on_message(client, userdata, msg):
    topic = msg.topic
    data = msg.payload.decode()
    
    if topic == "machine/start" and data!="True" and data!="False":
        event_collection.insert_many(device.get_machine_event("startRunning",randid,telemetry_data["count"],telemetry_data["speed"]))
        device.send_machine_event("startRunning",randid,telemetry_data["count"],telemetry_data["speed"])
        randid+=1
    elif topic == "machine/stop" and data!="True" and data!="False":
        event_collection.insert_many(device.get_machine_event("startRunning",randid,telemetry_data["count"],telemetry_data["speed"]))
        device.send_machine_event("startRunning",randid,telemetry_data["count"],telemetry_data["speed"])
        randid+=1
    elif topic == "machine/boxes":
        json_data = json.loads(data)
        telemetry_data["count"]=json_data["total_boxes"]
        client.publish("machine/boxcount",json_data["total_boxes"])
    elif topic =="machine/velocity":
        telemetry_data["speed"]=float(data)
        telemetry_collection.insert_many([device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])])
        device.send_telemetry(device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"]))
    elif topic=="machine/machineConsume":
        float_value = float(data)
        print("hoal")
        telemetry_collection.insert_many([device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])])
        device.send_telemetry(device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"]))
        

    
def on_connect(client, userdata, flags, rc):
    client.subscribe([("machine/machineConsume", 0), ("machine/start", 0), ("machine/velocity",0), ("machine/boxes",0), ("machine/stop",0)])

if __name__ == '__main__':
    client = mqtt.Client()
    client.connect("pi5", 1883)
    client.loop_start()
    client.on_connect = on_connect
    client.on_message = on_message
    
    
    try:
        caixes = 0
        while True:
            pass
        """for i in range(100):
            caixes+=1
            telemetry_collection.insert_many(data)
            device.send_telemetry(speed,caixes,0.2)
            sleep(1)"""
        """device.send_machine_event(
            event_type="startRunning",
            job_id="1",
            total_output_unit_count=140,
            machine_speed=5
        )"""
    except Exception as e:
        print("Error:", e)
    finally:
        device.close()
