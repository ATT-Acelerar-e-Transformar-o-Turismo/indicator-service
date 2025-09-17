from typing import List
from fastapi import APIRouter, HTTPException, Query
from bson.errors import InvalidId
from schemas.common import PyObjectId
from services.indicator_service import (
    create_indicator,
    get_all_indicators,
    get_indicators_count,
    get_indicator_by_id,
    get_indicators_by_domain,
    get_indicators_by_subdomain,
    update_indicator,
    delete_indicator,
    add_resource_to_indicator,
    remove_resource_from_indicator,
)
from schemas.indicator import IndicatorCreate, IndicatorUpdate, IndicatorPatch, Indicator, IndicatorDelete, SimpleIndicator
from schemas.resource import ResourceCreate
from config import settings
import logging

logger = logging.getLogger(__name__)

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


@router.get("/", response_model=List[SimpleIndicator])
async def get_indicators_route(
    skip: int = Query(0, ge=0), 
    limit: int = Query(10, ge=1, le=50),
    sort_by: str = Query("name", description="Field to sort by: name, periodicity, favourites"),
    sort_order: str = Query("asc", description="Sort order: asc or desc"),
    governance_filter: bool = Query(None, description="Filter by governance indicator: true/false")
):
    indicators = await get_all_indicators(skip=skip, limit=limit, sort_by=sort_by, sort_order=sort_order, governance_filter=governance_filter)
    return indicators


@router.get("/count", response_model=int)
async def get_indicators_count_route():
    """Get total count of indicators"""
    count = await get_indicators_count()
    return count


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


@router.get("/domain/{domain_id}/", response_model=List[SimpleIndicator])
async def get_indicators_by_domain_route(
    domain_id: str, 
    skip: int = Query(0, ge=0), 
    limit: int = Query(10, ge=1, le=50),
    sort_by: str = Query("name", description="Field to sort by: name, periodicity, favourites"),
    sort_order: str = Query("asc", description="Sort order: asc or desc"),
    governance_filter: bool = Query(None, description="Filter by governance indicator: true/false")
):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    try:
        indicators = await get_indicators_by_domain(domain_id, skip=skip, limit=limit, sort_by=sort_by, sort_order=sort_order, governance_filter=governance_filter)
        return indicators
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/domain/{domain_id}/subdomain/{subdomain_name}/", response_model=List[SimpleIndicator])
async def get_indicators_by_subdomain_route(
    domain_id: str, 
    subdomain_name: str, 
    skip: int = Query(0, ge=0), 
    limit: int = Query(10, ge=1, le=50),
    sort_by: str = Query("name", description="Field to sort by: name, periodicity, favourites"),
    sort_order: str = Query("asc", description="Sort order: asc or desc"),
    governance_filter: bool = Query(None, description="Filter by governance indicator: true/false")
):
    try:
        PyObjectId(domain_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_DOMAIN_ID)
    try:
        indicators = await get_indicators_by_subdomain(domain_id, subdomain_name, skip=skip, limit=limit, sort_by=sort_by, sort_order=sort_order, governance_filter=governance_filter)
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


@router.post("/{indicator_id}/resources", response_model=Indicator)
async def add_resource_route(indicator_id: str, resource: ResourceCreate):
    """Associate a resource to an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    try:
        updated_indicator = await add_resource_to_indicator(indicator_id, resource.resource_id)
        if not updated_indicator:
            raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
        return updated_indicator
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{indicator_id}/resources", response_model=List[str])
async def get_resources_route(indicator_id: str):
    """Get all resources for an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    try:
        indicator = await get_indicator_by_id(indicator_id)
        if not indicator:
            raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)

        return indicator.get("resources", [])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{indicator_id}/resources/{resource_id}", response_model=SimpleIndicator)
async def remove_resource_route(indicator_id: str, resource_id: str):
    """Remove association of a resource from an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    try:
        updated_indicator = await remove_resource_from_indicator(indicator_id, resource_id)
        if not updated_indicator:
            raise HTTPException(status_code=404, detail=NOT_FOUND_MESSAGE)
        return updated_indicator
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
