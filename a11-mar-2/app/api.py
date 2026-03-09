from fastapi import FastAPI, status
from .sensor_data import SensorData
from .mongo import MongoDB

app = FastAPI()

db = MongoDB()

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/readings", status_code=status.HTTP_201_CREATED)
def upload_readings(sensor_data: SensorData):
    mongo = MongoDB()
    mongo.upload_sensor_data(sensor_data)
    return  {"message": "Sensor data uploaded successfully"}