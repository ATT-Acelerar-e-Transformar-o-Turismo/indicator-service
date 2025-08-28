from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Response
from typing import List, Optional
from datetime import datetime
import re
from schemas.data_segment import DataPoint
from schemas.common import PyObjectId
from bson.errors import InvalidId
from services.data_propagator import get_data_points
from config import settings

router = APIRouter()

NOT_FOUND_MESSAGE = "Data not found"
INVALID_INDICATOR_ID = "Invalid indicator ID"
BASIC_AGGREGATORS = {"last", "first", "sum", "avg", "median", "max", "min", "count"}
PERCENTILE_PATTERN = re.compile(r"^p([0-9]|[1-9][0-9]|100)$")  # p0 to p100

def validate_aggregator(aggregator: str) -> bool:
    """Validate aggregator string"""
    return aggregator in BASIC_AGGREGATORS or bool(PERCENTILE_PATTERN.match(aggregator))


@router.get("/{indicator_id}/data", response_model=List[DataPoint])
async def get_indicator_data(
    indicator_id: str,
    background_tasks: BackgroundTasks,
    response: Response,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=10000),
    sort: str = Query("asc", regex="^(asc|desc)$"),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    granularity: str = Query("0", description="Granularity for data aggregation (e.g., 1s, 5m, 1h, 1d, 1w, 1M, 1y)"),
    aggregator: str = Query("last", description="Aggregation method (last, first, sum, avg, median, max, min, count, p0-p100)")
):
    """Get data points with optional filtering, pagination and aggregation"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)
    
    if not validate_aggregator(aggregator):
        raise HTTPException(status_code=400, detail=f"Invalid aggregator: {aggregator}. Valid options: {list(BASIC_AGGREGATORS)} or percentiles (p0-p100)")

    points = await get_data_points(
        indicator_id,
        skip=skip,
        limit=limit,
        sort=sort,
        granularity=granularity,
        aggregator=aggregator,
        start_date=start_date,
        end_date=end_date,
        background_tasks=background_tasks
    )
    response.headers["X-Total-Count"] = str(len(points))
    return points
