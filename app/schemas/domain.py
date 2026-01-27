from pydantic import BaseModel
from typing import List, Optional
from schemas.common import PyObjectId


class DomainBase(BaseModel):
    name: str
    color: str
    image: str
    icon: str = ""
    subdomains: List[str]


class DomainCreate(DomainBase):
    pass


class DomainUpdate(BaseModel):
    name: str
    color: str
    image: str
    icon: str = ""
    subdomains: List[str]


class DomainPatch(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    image: Optional[str] = None
    icon: Optional[str] = None
    subdomains: Optional[List[str]] = None


class Domain(DomainBase):
    id: PyObjectId

    class Config:
        from_attributes = True


class DomainDelete(BaseModel):
    id: str
    deleted: bool
