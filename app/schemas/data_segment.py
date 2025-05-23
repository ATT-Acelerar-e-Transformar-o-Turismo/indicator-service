from pydantic import BaseModel, Field
from typing import List, Dict
from datetime import datetime, UTC
from schemas.common import PyObjectId


class DataPoint(BaseModel):
    x: datetime | float
    y: float


class TimePoint(BaseModel):
    x: datetime
    y: float


class DataSegmentBase(BaseModel):
    indicator_id: PyObjectId
    points: List[TimePoint]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DataSegment(DataSegmentBase):
    class Config:
        from_attributes = True
