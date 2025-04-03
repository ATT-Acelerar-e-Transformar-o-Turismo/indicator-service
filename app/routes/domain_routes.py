from fastapi import APIRouter, HTTPException
from typing import List
from bson.errors import InvalidId
from schemas.common import PyObjectId
from services.domain_service import (
    get_all_domains,
    get_domain_by_id,
    create_domain,
    update_domain,
    patch_domain,
    delete_domain,
)
from schemas.domain import Domain, DomainCreate, DomainUpdate, DomainPatch, DomainDelete  # Import the delete schema

DOMAIN_NOT_FOUND = "Domain not found"
INVALID_DOMAIN_ID = "Invalid domain ID"

router = APIRouter()

@router.get("/", response_model=List[Domain])
async def get_domains():
    return await get_all_domains()

@router.get("/{domain_id}", response_model=Domain)
async def get_domain(domain_id: str):
    try:
        domain_id = PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    domain = await get_domain_by_id(domain_id)
    if not domain:
        raise HTTPException(status_code=404, detail=DOMAIN_NOT_FOUND)
    return domain

@router.post("/", response_model=Domain)
async def create_domain_route(domain: DomainCreate):
    created_domain = await create_domain(domain)
    if not created_domain:
        raise HTTPException(status_code=400, detail="Failed to create domain")
    return created_domain

@router.put("/{domain_id}", response_model=Domain)
async def update_domain_route(domain_id: str, domain: DomainUpdate):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    updated_domain = await update_domain(domain_id, domain)
    if not updated_domain:
        raise HTTPException(status_code=404, detail=DOMAIN_NOT_FOUND)
    return updated_domain

@router.patch("/{domain_id}", response_model=Domain)
async def patch_domain_route(domain_id: str, domain: DomainPatch):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    patched_domain = await patch_domain(domain_id, domain)
    if not patched_domain:
        raise HTTPException(status_code=404, detail=DOMAIN_NOT_FOUND)
    return patched_domain

@router.delete("/{domain_id}", response_model=DomainDelete)
async def delete_domain_route(domain_id: str):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    deleted_domain = await delete_domain(domain_id)
    if not deleted_domain:
        raise HTTPException(status_code=404, detail=DOMAIN_NOT_FOUND)
    return deleted_domain
