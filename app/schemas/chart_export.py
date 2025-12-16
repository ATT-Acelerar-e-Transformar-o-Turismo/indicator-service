from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum


class ChartType(str, Enum):
    line = "line"
    area = "area"
    bar = "bar"
    column = "column"
    scatter = "scatter"


class XAxisType(str, Enum):
    datetime = "datetime"
    category = "category"
    numeric = "numeric"


class ThemeMode(str, Enum):
    light = "light"
    dark = "dark"


class ChartExportRequest(BaseModel):
    # Data parameters
    granularity: str = Field("0", description="Data granularity")
    start_date: Optional[str] = Field(None, description="Start date (ISO format)")
    end_date: Optional[str] = Field(None, description="End date (ISO format)")

    # Chart configuration
    chart_type: ChartType = Field(ChartType.line, description="Type of chart")
    xaxis_type: XAxisType = Field(XAxisType.datetime, description="X-axis type")
    series: Optional[List[Any]] = Field(None, description="Series data (optional, fetched from DB if missing)")

    # Visual styling
    theme: ThemeMode = Field(default=ThemeMode.light, description="Chart theme")
    colors: Optional[List[str]] = Field(default=None, description="Color palette")
    
    # Chart features
    title: str = Field(default="", description="Chart title")
    show_legend: bool = Field(default=True, description="Show legend")
    show_annotations: bool = Field(default=True, description="Show annotations")
    annotations: Optional[Dict[str, Any]] = Field(default=None, description="Annotations configuration")
    logarithmic: Optional[int] = Field(default=None, description="Log scale")

    # Export options
    width: int = Field(default=800, description="Image width")
    height: int = Field(default=400, description="Image height")
    transparent_background: bool = Field(default=True, description="Transparent background")


class ChartExportResponse(BaseModel):
    image_data: str
    filename: str
    mime_type: str = "image/png"