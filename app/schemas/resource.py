from pydantic import BaseModel


class ResourceCreate(BaseModel):
    resource_id: str
