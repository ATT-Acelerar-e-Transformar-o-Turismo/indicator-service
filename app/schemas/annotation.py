from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Union
from enum import Enum
from datetime import datetime
from schemas.common import PyObjectId


class AnnotationType(str, Enum):
    xaxis = "xaxis"
    yaxis = "yaxis"


class Annotation(BaseModel):
    id: Optional[PyObjectId] = None
    indicator_id: PyObjectId
    type: AnnotationType
    value: Union[float, datetime, str]  # timestamp for xaxis, numeric for yaxis
    label: str
    color: str = "#000000"
    stroke_dasharray: int = 0  # 0 = solid line
    opacity: float = Field(default=1.0, ge=0.0, le=1.0)

    @field_validator('value')
    @classmethod
    def validate_value(cls, v, info):
        if isinstance(v, str):
            try:
                # Handle various datetime formats
                if 'T' in v:
                    # Clean up the datetime string
                    cleaned = v.replace('Z+00:00', '+00:00').replace('Z', '+00:00')
                    return datetime.fromisoformat(cleaned)
                else:
                    # Try parsing as float
                    return float(v)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid value format: {v}")
        return v

    class Config:
        from_attributes = True


class AnnotationCreate(BaseModel):
    type: AnnotationType
    value: Union[float, datetime, str]
    label: str
    color: str = "#000000"
    stroke_dasharray: int = 0
    opacity: float = Field(default=1.0, ge=0.0, le=1.0)

    @field_validator('value')
    @classmethod
    def validate_value(cls, v, info):
        if isinstance(v, str):
            try:
                # Handle various datetime formats
                if 'T' in v:
                    # Clean up the datetime string
                    cleaned = v.replace('Z+00:00', '+00:00').replace('Z', '+00:00')
                    return datetime.fromisoformat(cleaned)
                else:
                    # Try parsing as float
                    return float(v)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid value format: {v}")
        return v


class AnnotationUpdate(BaseModel):
    value: Optional[Union[float, datetime, str]] = None
    label: Optional[str] = None
    color: Optional[str] = None
    stroke_dasharray: Optional[int] = None
    opacity: Optional[float] = None

    @field_validator('value')
    @classmethod
    def validate_value(cls, v, info):
        if v is None:
            return v
        if isinstance(v, str):
            try:
                # Handle various datetime formats
                if 'T' in v:
                    # Clean up the datetime string
                    cleaned = v.replace('Z+00:00', '+00:00').replace('Z', '+00:00')
                    return datetime.fromisoformat(cleaned)
                else:
                    # Try parsing as float
                    return float(v)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid value format: {v}")
        return v