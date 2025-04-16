from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict
from datetime import datetime
from schemas.data_segment import DataPoint
from schemas.common import PyObjectId
from bson.errors import InvalidId

router = APIRouter()

NOT_FOUND_MESSAGE = "Data not found"
INVALID_INDICATOR_ID = "Invalid indicator ID"


@router.get("/{indicator_id}/data/by-date", response_model=List[DataPoint])
async def get_indicator_data(
    indicator_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: int = Query(100, ge=1, le=10000)
):
    """Get data points for an indicator with optional date range filter"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    # Logic will be implemented in service layer
    raise HTTPException(status_code=501, detail="Not implemented")


@router.get("/{indicator_id}/data", response_model=List[DataPoint])
async def get_paginated_data(
    indicator_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=10000),
    sort: str = Query("asc", regex="^(asc|desc)$")
):
    """Get paginated data points with metadata"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    # Logic will be implemented in service layer
    raise HTTPException(status_code=501, detail="Not implemented")
