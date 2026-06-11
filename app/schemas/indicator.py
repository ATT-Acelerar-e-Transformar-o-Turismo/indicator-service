from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict, Literal
from schemas.domain import Domain
from schemas.common import PyObjectId
from schemas.chart_export import ChartType
from utils.formula import compile_formula, FormulaError


class SeriesTranslation(BaseModel):
    """PT/EN labels for a single data column (series_label).

    Seeded by the resource-service Gemini sidecar, editable in the admin
    indicator wizard. Either side may be empty if the source language is
    already the target.
    """

    pt: str = ""
    en: str = ""


CompositionBucket = Literal["1d", "1w", "1M", "3M", "1y"]
CompositionAggregator = Literal["avg", "sum", "min", "max", "last", "first", "count", "median"]


class CompositionInput(BaseModel):
    """One operand of a composed-indicator formula.

    `key` is the identifier used in the formula expression (e.g. "a", "b").
    `indicator_id` is the source whose data feeds that key.
    """

    key: str = Field(..., min_length=1, max_length=8)
    indicator_id: PyObjectId

    @field_validator("key")
    @classmethod
    def _key_is_identifier(cls, v: str) -> str:
        if not v.isidentifier() or v.startswith("_"):
            raise ValueError("key must be a valid identifier (letters/digits, no leading underscore)")
        # Disallow shadowing whitelisted functions / Python keywords
        reserved = {"min", "max", "abs", "round", "log", "log2", "log10", "sqrt", "exp", "pow"}
        if v in reserved:
            raise ValueError(f"key `{v}` is reserved")
        return v


class CompositionCreate(BaseModel):
    """Payload for creating a composition on an indicator."""

    name: Optional[str] = None
    name_en: Optional[str] = None
    inputs: List[CompositionInput]
    formula: str = Field(..., min_length=1, max_length=512)
    bucket: CompositionBucket = "1M"
    aggregator: CompositionAggregator = "avg"

    @field_validator("inputs")
    @classmethod
    def _unique_keys_and_nonempty(cls, v: List[CompositionInput]) -> List[CompositionInput]:
        if len(v) != 2:
            raise ValueError("inputs must contain exactly two entries")
        keys = [i.key for i in v]
        if len(set(keys)) != len(keys):
            raise ValueError("input keys must be unique")
        return v

    @model_validator(mode="after")
    def _validate_formula(self) -> "CompositionCreate":
        try:
            compile_formula(self.formula, [i.key for i in self.inputs])
        except FormulaError as e:
            raise ValueError(f"Invalid formula: {e}") from e
        return self


class Composition(CompositionCreate):
    """Stored composition — same fields plus a stable id.

    The formula validator is not re-run when reading from the database because
    the formula was already validated at write time and re-compiling on every
    read is wasteful (and could fail on Pydantic model_validator mode changes).
    """

    id: str

    @model_validator(mode="after")
    def _validate_formula(self) -> "Composition":
        # Override parent to skip formula re-validation on read.
        return self


DEFAULT_CHART_TYPES: List[ChartType] = [
    ChartType.line,
    ChartType.column,
    ChartType.bar,
    ChartType.scatter,
]
DEFAULT_CHART_TYPE: ChartType = ChartType.line


IndicatorStatus = Literal["draft", "published"]


class IndicatorBase(BaseModel):
    name: str
    name_en: Optional[str] = ""
    # Drafts can be saved with only name + area + dimension, so periodicity
    # and favourites need defaults — the strict required-field validation
    # used to reject "Guardar rascunho" submissions outright.
    periodicity: Optional[str] = ""
    periodicity_en: Optional[str] = ""
    favourites: int = 0
    governance: bool = False
    # Lifecycle flag. Drafts are hidden from public listings and from the
    # admin's default view; the admin opts into seeing them via the
    # "Rascunhos" pill (status_filter=draft on the API).
    status: IndicatorStatus = "published"
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
    # Derived series defined by a free-form formula over one or more existing
    # indicators (e.g. "a / b"). Each composition produces a single chart line
    # alongside the indicator's own resources / child indicators.
    compositions: List[Composition] = Field(default_factory=list)

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
    # Promote draft → published (or demote, though the UI doesn't do that).
    status: Optional[IndicatorStatus] = None
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
