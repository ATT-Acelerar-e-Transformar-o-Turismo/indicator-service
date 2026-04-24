from pydantic import BaseModel, field_validator
from typing import List, Optional, Union
from schemas.common import PyObjectId


class Subdomain(BaseModel):
    name: str
    name_en: Optional[str] = ""
    hidden: bool = False


class DomainBase(BaseModel):
    name: str
    name_en: Optional[str] = ""
    color: str
    image: Optional[str] = ""
    icon: Optional[str] = ""
    hidden: bool = False
    subdomains: List[Union[Subdomain, str]]

    @field_validator("subdomains", mode="before")
    @classmethod
    def normalize_subdomains(cls, v):
        result = []
        for item in v:
            if isinstance(item, str):
                result.append(Subdomain(name=item))
            elif isinstance(item, dict):
                result.append(Subdomain(**item))
            else:
                result.append(item)
        return result


class DomainCreate(DomainBase):
    pass


class DomainUpdate(BaseModel):
    name: str
    name_en: Optional[str] = ""
    color: str
    image: Optional[str] = ""
    icon: Optional[str] = ""
    hidden: bool = False
    subdomains: List[Union[Subdomain, str]]

    @field_validator("subdomains", mode="before")
    @classmethod
    def normalize_subdomains(cls, v):
        result = []
        for item in v:
            if isinstance(item, str):
                result.append(Subdomain(name=item))
            elif isinstance(item, dict):
                result.append(Subdomain(**item))
            else:
                result.append(item)
        return result


class DomainPatch(BaseModel):
    name: Optional[str] = None
    name_en: Optional[str] = None
    color: Optional[str] = None
    image: Optional[str] = None
    icon: Optional[str] = None
    hidden: Optional[bool] = None
    subdomains: Optional[List[Union[Subdomain, str]]] = None

    @field_validator("subdomains", mode="before")
    @classmethod
    def normalize_subdomains(cls, v):
        if v is None:
            return v
        result = []
        for item in v:
            if isinstance(item, str):
                result.append(Subdomain(name=item))
            elif isinstance(item, dict):
                result.append(Subdomain(**item))
            else:
                result.append(item)
        return result


class Domain(DomainBase):
    id: PyObjectId

    class Config:
        from_attributes = True


class DomainDelete(BaseModel):
    id: str
    deleted: bool
