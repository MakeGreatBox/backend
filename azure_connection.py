
from azure.iot.device import IoTHubDeviceClient, Message
from datetime import datetime, timezone
import json

class IoTDevice:
    CONNECTION_STRING = "HostName=ra-develop-bobstconnect-01.azure-devices.net;DeviceId=LAUZHACKPI5;SharedAccessKey=cRbJIBv9IJEqd0W60+ogh0+ya+jTLVc34AIoTL+GEEY="
    MACHINE_ID = "lauzhack-pi5"
    DATA_IP = "10.0.4.95:80"

    def __init__(self):
        self.create_device_client()
        self.machine_id = self.MACHINE_ID
        self.PROC_ID:int = 1

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
        telemetry_data = self.get_json_telemetry(speed, count, energy)
        self.__send_message(telemetry_data,"Telemetry")
    
    def get_machine_event(self, event_type:str, job_id, total_output_unit_count, machine_speed):
        return {
            "type": event_type,
            "equipmentId": self.machine_id,
            "jobId": job_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "machineSpeed": machine_speed,
            "totaloutputunitcount": total_output_unit_count,
        }

    def send_machine_event(self, event_type:str, job_id, total_output_unit_count, machine_speed):
        machine_event = self.get_machine_event(event_type, job_id, total_output_unit_count, machine_speed)
        self.__send_message([machine_event], "MachineEvent")
        self.increase_id()

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
