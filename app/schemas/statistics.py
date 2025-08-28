from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class IndicatorStatistics(BaseModel):
    count: int
    min_x: Optional[datetime | float] = None
    max_x: Optional[datetime | float] = None
    avg_y: Optional[float] = None
    min_y: Optional[float] = None
    max_y: Optional[float] = None

    class Config:
        from_attributes = True