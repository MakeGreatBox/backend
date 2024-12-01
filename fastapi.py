from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

MONGO_URL="mongodb://localhost:27017"
DB_NAME="my_mongo"
COLLECTION_NAME=""

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.put("/emergency_stop/")
def create_item():
    pass

@app.get("/data/hour/")
def get_data():
    return [{"tonto":"hola"}]