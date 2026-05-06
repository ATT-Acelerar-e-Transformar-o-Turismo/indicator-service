from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime, UTC
from schemas.common import PyObjectId


class DataPoint(BaseModel):
    x: datetime | float
    y: float
    # Optional series label. Multi-column files (one resource → many series)
    # tag each emitted point with the column name; single-source resources
    # leave it None.
    series: Optional[str] = None


class TimePoint(BaseModel):
    x: datetime
    y: float
    series: Optional[str] = None


class DataSegmentBase(BaseModel):
    indicator_id: PyObjectId
    resource_id: PyObjectId
    points: List[TimePoint]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DataSegment(DataSegmentBase):
    class Config:
        from_attributes = True


class MergedIndicator(BaseModel):
    indicator_id: PyObjectId
    points: List[DataPoint]
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))

    class Config:
        from_attributes = True


class IndicatorSeries(BaseModel):
    """One line on the chart. Identified by (resource_id, series_label):
    a single resource can contribute several series when the source file has
    multiple value columns. series_label is None for legacy single-stream
    resources (one wrapper → one untagged series)."""
    resource_id: str
    series_label: Optional[str] = None
    points: List[DataPoint]
