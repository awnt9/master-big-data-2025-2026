from pymongo import MongoClient
from config import Settings

settings = Settings()

class MongoDB:
    def __init__(self):
        self.client = MongoClient(
            host = settings.mongo_ip,
            port = settings.mongo_port,
            username = settings.mongo_username,
            password = settings.mongo_root_password,
        )
        self.db = "sensorhub"
        self.collection = "sensor_data"

    def upload_sensor_data(self, data: dict):

        self.client.get_database(self.db).get_collection(self.collection).insert_one(data)

    def read_sensor_data(self):
        return self.client.get_database(self.db).get_collection(self.collection).find({})
