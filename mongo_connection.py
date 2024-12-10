from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "my_mongo"

class MondongoDB():
    db = MongoClient(MONGO_URI)[DB_NAME]
    telemetry = db["telemetry"]
    machine_events = db["machine_events"]