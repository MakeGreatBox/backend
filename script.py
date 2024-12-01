import datetime
import json
from azure.iot.device import IoTHubDeviceClient, Message
from time import sleep
CONNECTION_STRING = "HostName=ra-develop-bobstconnect-01.azure-devices.net;DeviceId=LAUZHACKPI5;SharedAccessKey=cRbJIBv9IJEqd0W60+ogh0+ya+jTLVc34AIoTL+GEEY="
MACHINE_ID = "lauzhack-pi5"
DATA_IP = "10.0.4.95:80"


class IoTDevice:
    def __init__(self, connection_string, machine_id):
        self.create_device_client(connection_string)
        self.machine_id = machine_id

    def create_device_client(self,connection_string):
        try:
            self.client = IoTHubDeviceClient.create_from_connection_string(connection_string)
            self.client.connect()
            print("Successfully connected to IoT Hub!")
        except Exception as e:
            print(f"Error connecting to IoT Hub: {e}")
    
    def send_telemetry(self, speed, count, energy):
        telemetry_data = {
            "telemetry" : {
                "machineid": MACHINE_ID,
                "datasource": DATA_IP,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "machinespeed" : speed,
                "totaloutputunitcount" : count,
                "totalworkingenergy": energy
            }
        }
    
        self.__send_message(telemetry_data,"Telemetry")
    

    def send_machine_event(self, event_type, job_id, total_output_unit_count, machine_speed):

        machine_event = {
            "type": event_type,
            "equipmentId": self.machine_id,
            "jobId": job_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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



# Testing the code
if __name__ == "__main__":
    device = IoTDevice(CONNECTION_STRING, MACHINE_ID)
    try:
        caixes = 0
        for i in range(100):
            caixes+=1
            device.send_telemetry(0.3,caixes,0.2)
            sleep(1)
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
