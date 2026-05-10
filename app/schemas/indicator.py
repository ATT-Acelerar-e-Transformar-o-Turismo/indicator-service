from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict
from schemas.domain import Domain
from schemas.common import PyObjectId
from schemas.chart_export import ChartType


class SeriesTranslation(BaseModel):
    """PT/EN labels for a single data column (series_label).

    Seeded by the resource-service Gemini sidecar, editable in the admin
    indicator wizard. Either side may be empty if the source language is
    already the target.
    """

    pt: str = ""
    en: str = ""


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
    # Series labels (column names from data wrappers) the admin has toggled
    # off. The public chart filters these out so the data stays in storage but
    # is hidden from end users — same lever as the wrapper-time column picker
    # but applied post hoc, without re-running the wrapper.
    hidden_series: List[str] = Field(default_factory=list)
    # PT/EN translations for each series_label, keyed by the original label
    # from the data wrapper. Seeded by resource-service after wrapper
    # completion (Gemini sidecar) and editable in the admin wizard.
    series_translations: Dict[str, SeriesTranslation] = Field(default_factory=dict)
    # Other indicators whose data should appear on this indicator's chart
    # alongside its own resources. Each child contributes its full series
    # tree (transitive); cycles are rejected at write time and a depth cap
    # protects against runtime regressions. Empty for non-composed
    # indicators.
    child_indicators: List[str] = Field(default_factory=list)

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
    hidden_series: Optional[List[str]] = None
    series_translations: Optional[Dict[str, SeriesTranslation]] = None
    # NOTE: `child_indicators` deliberately omitted — composed indicators are
    # managed via POST/DELETE /{id}/child-indicators which run cycle / self-
    # inclusion / existence checks. Letting PATCH overwrite the list would
    # bypass those guards.

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
