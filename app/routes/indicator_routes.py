from typing import List
from fastapi import APIRouter, HTTPException, Query
from bson.errors import InvalidId
from schemas.common import PyObjectId
from services.indicator_service import (
    create_indicator,
    get_all_indicators,
    get_indicator_by_id,
    get_indicators_by_domain,
    get_indicators_by_subdomain,
    update_indicator,
    delete_indicator,
)
from schemas.indicator import IndicatorCreate, IndicatorUpdate, IndicatorPatch, Indicator, IndicatorDelete

router = APIRouter()

NOT_FOUND_MESSAGE = "Indicator not found"
INVALID_DOMAIN_ID = "Invalid domain ID"
INVALID_INDICATOR_ID = "Invalid indicator ID"

@router.post("/{domain_id}/{subdomain_name}/", response_model=Indicator)
async def create_indicator_route(domain_id: str, subdomain_name: str, indicator: IndicatorCreate):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    try:
        indicator_data = await create_indicator(domain_id, subdomain_name, indicator)
        return indicator_data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/", response_model=List[Indicator])
async def get_indicators_route(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=50)):
    indicators = await get_all_indicators(skip=skip, limit=limit)
    return indicators

@router.get("/{indicator_id}", response_model=Indicator)
async def get_indicator_route(indicator_id: str):
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)
    indicator = await get_indicator_by_id(indicator_id)
    if not indicator:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
    return indicator

@router.get("/domain/{domain_id}", response_model=List[Indicator])
async def get_indicators_by_domain_route(
    domain_id: str, skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=50)
):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    try:
        indicators = await get_indicators_by_domain(domain_id, skip=skip, limit=limit)
        return indicators
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/domain/{domain_id}/subdomain/{subdomain_name}", response_model=List[Indicator])
async def get_indicators_by_subdomain_route(
    domain_id: str, subdomain_name: str, skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=50)
):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    try:
        indicators = await get_indicators_by_subdomain(domain_id, subdomain_name, skip=skip, limit=limit)
        return indicators
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.put("/{indicator_id}", response_model=Indicator)
async def update_indicator_route(indicator_id: str, indicator: IndicatorUpdate):
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)
    try:
        updated_count = await update_indicator(indicator_id, indicator.dict(exclude_unset=True))
        if updated_count == 0:
            raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
        updated_indicator = await get_indicator_by_id(indicator_id)
        return updated_indicator
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.patch("/{indicator_id}", response_model=Indicator)
async def patch_indicator_route(indicator_id: str, indicator: IndicatorPatch):
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)
    try:
        updated_count = await update_indicator(indicator_id, indicator.dict(exclude_unset=True))
        if updated_count == 0:
            raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
        updated_indicator = await get_indicator_by_id(indicator_id)
        return updated_indicator
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{indicator_id}", response_model=IndicatorDelete)
async def delete_indicator_route(indicator_id: str):
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)
    deleted_indicator = await delete_indicator(indicator_id)
    if not deleted_indicator:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
    return deleted_indicator
