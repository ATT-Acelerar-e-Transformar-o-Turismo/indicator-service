from typing import List, Union, Dict, Tuple
from datetime import datetime, timedelta
import json
from schemas.data_segment import DataPoint, TimePoint
from dependencies.database import get_database
from dependencies.redis import redis_client
from bson.objectid import ObjectId
import logging

logger = logging.getLogger(__name__)

CACHE_KEY_PREFIX = "indicator_data:"
CACHE_TTL = timedelta(hours=1)  # Cache expiration time


def get_cache_key(indicator_id: str) -> str:
    return f"{CACHE_KEY_PREFIX}{indicator_id}"


async def update_cached_data(indicator_id: str) -> List[DataPoint]:
    """Process and cache indicator data"""
    db = get_database()
    segments = await db.data_segments.find(
        {"indicator_id": ObjectId(indicator_id)}
    ).to_list(None)

    logger.info(f"Updating cached data for indicator {indicator_id}")
    logger.info(f"Found {len(segments)} segments")
    logger.info(f"Segments: {segments}")

    time_series_points: Dict[datetime, Tuple[float, datetime]] = {}
    numeric_points = []

    for segment in segments:
        segment_timestamp = segment.get("timestamp")
        if not segment_timestamp:
            continue

        for point in segment["points"]:
            if isinstance(point["x"], (str, datetime)):
                x_value = datetime.fromisoformat(point["x"].replace(
                    'Z', '+00:00')) if isinstance(point["x"], str) else point["x"]

                current = time_series_points.get(x_value)
                if not current or segment_timestamp > current[1]:
                    time_series_points[x_value] = (
                        point["y"], segment_timestamp)
            else:
                numeric_points.append(
                    {"x": float(point["x"]), "y": float(point["y"])})

    time_points = [{"x": x.isoformat(), "y": y}
                   for x, (y, _) in time_series_points.items()]

    # Sort all points in ascending order
    sorted_points = sorted(
        time_points + numeric_points,
        key=lambda p: p["x"]
    )

    # Cache the processed data
    cache_key = get_cache_key(indicator_id)
    await redis_client.set(
        cache_key,
        json.dumps(sorted_points),
        ex=int(CACHE_TTL.total_seconds())
    )

    return sorted_points


async def get_sorted_data_points(
    indicator_id: str,
    skip: int = 0,
    limit: int = 50,
    sort: str = "asc"
) -> List[DataPoint]:
    """Get sorted data points for an indicator using cache"""
    cache_key = get_cache_key(indicator_id)
    cached_data = await redis_client.get(cache_key)

    if cached_data:
        points = json.loads(cached_data)
    else:
        points = await update_cached_data(indicator_id)

    # Apply sort and pagination
    if sort == "desc":
        points = list(reversed(points))

    return points[skip:skip + limit]


async def get_data_points_by_date(
    indicator_id: str,
    start_date: datetime = None,
    end_date: datetime = None,
    limit: int = 100
) -> List[DataPoint]:
    """Get data points filtered by date range using cache"""
    cache_key = get_cache_key(indicator_id)
    cached_data = await redis_client.get(cache_key)

    if cached_data:
        points = json.loads(cached_data)
    else:
        points = await update_cached_data(indicator_id)

    # Filter by date range
    if start_date or end_date:
        filtered_points = []
        for point in points:
            if isinstance(point["x"], str):
                point_date = datetime.fromisoformat(
                    point["x"].replace('Z', '+00:00'))
                if start_date and point_date < start_date:
                    continue
                if end_date and point_date > end_date:
                    continue
            filtered_points.append(point)
        points = filtered_points

    return points[:limit]
