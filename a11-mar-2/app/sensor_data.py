from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class SensorData(BaseModel):
    device_id: str
    location: str
    temperature: str
    humidity: str
    co2: str
    timestamp: Optional[str] = datetime.now().isoformat()
