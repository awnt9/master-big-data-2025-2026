from pymongo import MongoClient
from config import Settings
from sensor_data import SensorData

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

        self.client_collection = self.client.get_database(self.db).get_collection(self.collection)

    def upload_sensor_data(self, sensor_data:SensorData):
        sensor_data_dict = sensor_data.model_dump()
        self.client_collection.insert_one(sensor_data_dict)

    def read_sensor_data(self,device_id,limit):
        filter = {"device_id": device_id} if device_id else {}
        
        

        return self.client.get_database(self.db).get_collection(self.collection).find({})