from datetime import datetime, timezone
from pydantic import BaseModel

from azure_connection import IoTDevice
from mongo_connection import MondongoDB
from mqtt_connection  import Mqtt

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

device = IoTDevice()
mondongo = MondongoDB()

telemetry_data = {"speed":0.20,"count":0,"energy":0}

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify allowed origins here
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
)

client = Mqtt([("machine/machineConsume", 0), 
               ("machine/start", 0), 
               ("machine/velocity",0), 
               ("machine/boxes",0), 
               ("machine/stop",0)],
               mondongo,
               device,
               telemetry_data)
class State(BaseModel):
    state: bool
    
@app.post("/buttonState/")
def create_item(state:State):
    event = "start" if state.state else "stop"
    mondongo.machine_events.insert_many([device.get_machine_event(f"{event}Producing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])])
    device.send_machine_event(f"{event}Producing",device.PROC_ID,telemetry_data["count"],telemetry_data["speed"])
    client.publish(f"machine/{event}",True)

@app.post("/speed/{value}/")
def get_data(value:int):
    mapper = {1:(0.15,0.2),2:(0.23,0.25), 3:(0.25,0.27)}

    speed_azure, speed_mqtt = mapper[value]
    client.publish("machine/velocity",speed_mqtt)
    telemetry_data["speed"]=speed_azure
    mondongo.telemetry.insert_many([device.get_json_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])])
    device.send_telemetry(telemetry_data["speed"],telemetry_data["count"],telemetry_data["energy"])

@app.get("/cosa/cosa/")
def get_story():
    stats = mondongo.machine_events.find({"type":"stopProducing"})
    for i in stats:
        print(i["timestamp"])
        next_document = mondongo.machine_events.find({"timestamp": {"$gt": i["timestamp"]}}).sort("timestamp", 1).limit(1)
        time = next_document["timestamp"]-i["timestamp"]