from pymongo import MongoClient
from app.config import Settings
from .sensor_data import SensorData

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

    def upload_sensor_data(self, sendor_data: SensorData):
        sensor_data_dict = sendor_data.model_dump()
        self.client_collection.insert_one(sensor_data_dict)

    def read_sensor_data(self, device_id: str = None, max_records: int = None):
        filter = {"device_id": device_id} if device_id else {}
        cursor = self.client_collection.find(filter, limit=max_records)
        return cursor


if __name__ == "__main__":

    db = MongoDB()

    sensordata = SensorData(
        device_id="antonio",
        location="office",
        temperature=25.5,
        humidity=60.0,
        co2="400.0",
        antonio="antonio"
    )

    db.upload_sensor_data(sensordata)
    print(list(db.read_sensor_data(device_id="antonio", max_records=1)))