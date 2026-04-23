from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ChartType(str, Enum):
    line = "line"
    area = "area"
    bar = "bar"
    column = "column"
    scatter = "scatter"
    # Aggregate visualizations: derived from {x,y} time-series data
    pie = "pie"
    donut = "donut"
    treemap = "treemap"
    heatmap = "heatmap"
    # Shape-specific types: require specially-structured series data
    # (box plot: [min,q1,median,q3,max]; candlestick: [open,high,low,close];
    # range bar / range area: [low, high]). When the underlying data does
    # not match the required shape, the renderer falls back to an empty
    # chart carrying the message "Chart type not supported for this data".
    boxPlot = "boxPlot"
    candlestick = "candlestick"
    rangeBar = "rangeBar"
    rangeArea = "rangeArea"


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
    start_date: Optional[datetime] = Field(None, description="Start date (ISO format)")
    end_date: Optional[datetime] = Field(None, description="End date (ISO format)")

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