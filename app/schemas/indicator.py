from pydantic import BaseModel, Field
from typing import Optional
from schemas.domain import Domain
from schemas.common import PyObjectId

class IndicatorBase(BaseModel):
    name: str
    periodicity: str
    favourites: int
    governance: bool
    description: Optional[str] = None
    font: Optional[str] = None
    scale: Optional[str] = None

class IndicatorCreate(IndicatorBase):
    pass

class IndicatorUpdate(IndicatorBase):
    domain: PyObjectId
    subdomain: str

class IndicatorPatch(BaseModel):
    name: Optional[str] = None
    periodicity: Optional[str] = None
    domain: Optional[PyObjectId] = None
    subdomain: Optional[str] = None
    favourites: Optional[int] = None
    governance: Optional[bool] = None
    description: Optional[str] = None
    font: Optional[str] = None
    scale: Optional[str] = None

class Indicator(IndicatorBase):
    id: PyObjectId
    domain: Domain
    subdomain: str

    class Config:
        orm_mode = True

class IndicatorDelete(BaseModel):
    id: str
    deleted: bool
