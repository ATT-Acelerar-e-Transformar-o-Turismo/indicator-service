from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
from schemas.domain import Domain
from schemas.common import PyObjectId
from schemas.chart_export import ChartType


DEFAULT_CHART_TYPES: List[ChartType] = [
    ChartType.line,
    ChartType.column,
    ChartType.bar,
    ChartType.scatter,
]
DEFAULT_CHART_TYPE: ChartType = ChartType.line


class IndicatorBase(BaseModel):
    name: str
    name_en: Optional[str] = ""
    periodicity: str
    periodicity_en: Optional[str] = ""
    favourites: int
    governance: bool
    description: Optional[str] = None
    description_en: Optional[str] = ""
    font: Optional[str] = None
    font_en: Optional[str] = ""
    scale: Optional[str] = None
    scale_en: Optional[str] = ""
    unit: Optional[str] = None
    unit_en: Optional[str] = ""
    carrying_capacity: Optional[str] = None
    chart_types: List[ChartType] = Field(default_factory=lambda: list(DEFAULT_CHART_TYPES))
    default_chart_type: ChartType = DEFAULT_CHART_TYPE

    @field_validator("chart_types")
    @classmethod
    def _chart_types_non_empty(cls, v: List[ChartType]) -> List[ChartType]:
        if not v:
            raise ValueError("chart_types must contain at least one chart type")
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for t in v:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        return unique

    @model_validator(mode="after")
    def _default_in_chart_types(self) -> "IndicatorBase":
        if self.default_chart_type not in self.chart_types:
            raise ValueError(
                "default_chart_type must be one of the values in chart_types"
            )
        return self


class IndicatorCreate(IndicatorBase):
    pass


class IndicatorUpdate(IndicatorBase):
    domain: PyObjectId
    subdomain: str
    hidden: bool = False


class IndicatorPatch(BaseModel):
    name: Optional[str] = None
    name_en: Optional[str] = None
    periodicity: Optional[str] = None
    periodicity_en: Optional[str] = None
    domain: Optional[PyObjectId] = None
    subdomain: Optional[str] = None
    favourites: Optional[int] = None
    governance: Optional[bool] = None
    hidden: Optional[bool] = None
    description: Optional[str] = None
    description_en: Optional[str] = None
    font: Optional[str] = None
    font_en: Optional[str] = None
    scale: Optional[str] = None
    scale_en: Optional[str] = None
    unit: Optional[str] = None
    unit_en: Optional[str] = None
    carrying_capacity: Optional[str] = None
    chart_types: Optional[List[ChartType]] = None
    default_chart_type: Optional[ChartType] = None

    @model_validator(mode="after")
    def _patch_chart_consistency(self) -> "IndicatorPatch":
        if self.chart_types is not None and len(self.chart_types) == 0:
            raise ValueError("chart_types must contain at least one chart type")
        if (
            self.chart_types is not None
            and self.default_chart_type is not None
            and self.default_chart_type not in self.chart_types
        ):
            raise ValueError(
                "default_chart_type must be one of the values in chart_types"
            )
        return self


class Indicator(IndicatorBase):
    id: PyObjectId
    hidden: bool = False
    domain: Domain
    subdomain: str
    resources: List[str] = Field(default_factory=list)  # List of resource IDs

    class Config:
        from_attributes = True


class SimpleIndicator(IndicatorBase):
    id: PyObjectId
    hidden: bool = False
    domain: PyObjectId
    subdomain: str
    resources: List[str] = Field(default_factory=list)  # List of resource IDs

    class Config:
        from_attributes = True


class IndicatorDelete(BaseModel):
    id: str
    deleted: bool
