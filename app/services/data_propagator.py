from typing import List, Union, Dict, Optional, Any
from datetime import datetime, timedelta
import json
import re
import time
from schemas.data_segment import DataPoint
from dependencies.database import db
from dependencies.redis import redis_client
from bson.objectid import ObjectId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
import logging
from fastapi import BackgroundTasks
from config import settings

logger = logging.getLogger(__name__)


# (amount, unit, approx_seconds) — sorted ascending by bucket size
_AUTO_BUCKETS = [
    (1, "s", 1),
    (5, "s", 5),
    (15, "s", 15),
    (30, "s", 30),
    (1, "m", 60),
    (5, "m", 300),
    (15, "m", 900),
    (1, "h", 3600),
    (6, "h", 21600),
    (1, "d", 86400),
    (1, "w", 604800),
    (1, "M", 2592000),
    (3, "M", 7776000),
    (1, "y", 31536000),
    (5, "y", 157680000),
]
_AUTO_GRANULARITY_TTL = 300  # seconds
_auto_granularity_cache: Dict[tuple, tuple] = {}


async def resolve_auto_granularity(indicator_id: str, target: int) -> str:
    """Pick a bucket size that yields at most `target` points for this indicator's data span.

    Returns a granularity string (e.g. "1M", "1y") or "0" if raw fits within target.
    Cached in-process for _AUTO_GRANULARITY_TTL seconds per (indicator_id, target).
    """
    if target <= 0:
        return "0"

    cache_key = (indicator_id, target)
    cached = _auto_granularity_cache.get(cache_key)
    now = time.monotonic()
    if cached and (now - cached[1]) < _AUTO_GRANULARITY_TTL:
        return cached[0]

    try:
        pipeline = [
            {"$match": {"indicator_id": ObjectId(indicator_id)}},
            {"$unwind": "$points"},
            {"$group": {
                "_id": None,
                "min_x": {"$min": "$points.x"},
                "max_x": {"$max": "$points.x"},
                "n": {"$sum": 1},
            }},
        ]
        result = await db.merged_indicators.aggregate(pipeline).to_list(1)
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error resolving auto granularity for {indicator_id}: {e}")
        return "0"

    if not result:
        return "0"

    info = result[0]
    n = info.get("n", 0)
    min_x, max_x = info.get("min_x"), info.get("max_x")
    if n <= target or not isinstance(min_x, datetime) or not isinstance(max_x, datetime):
        resolved = "0"
    else:
        span_seconds = max(1.0, (max_x - min_x).total_seconds())
        bucket_seconds = span_seconds / target
        chosen = _AUTO_BUCKETS[-1]
        for amount, unit, secs in _AUTO_BUCKETS:
            if secs >= bucket_seconds:
                chosen = (amount, unit, secs)
                break
        resolved = f"{chosen[0]}{chosen[1]}"

    _auto_granularity_cache[cache_key] = (resolved, now)
    return resolved



def get_cache_key(indicator_id: str, granularity: str = "0", aggregator: str = "last", **extra_params) -> str:
    """Generate cache key with aggregator and optional extra parameters"""
    parts = [indicator_id, granularity, aggregator]
    
    if extra_params:
        # Sort for consistent key generation
        sorted_extras = sorted(f"{k}:{v}" for k, v in extra_params.items())
        parts.extend(sorted_extras)
    
    return f"{settings.CACHE_KEY_PREFIX}{':'.join(parts)}"

def get_counter_key(indicator_id: str, granularity: str = "0", aggregator: str = "last") -> str:
    return f"{settings.CACHE_COUNTER_PREFIX}{indicator_id}:{granularity}:{aggregator}:counter"


def parse_granularity(granularity: str) -> tuple[int, str]:
    """Parse granularity string like '5m', '1h', '2d' into (amount, unit)"""
    if granularity == "0" or not granularity:
        return 0, ""

    match = re.match(r"^(\d+)([smhdwMy])$", granularity)
    if not match:
        raise ValueError(f"Invalid granularity format: {granularity}")

    amount, unit = match.groups()
    return int(amount), unit


def parse_aggregator(aggregator: str) -> tuple[str, Optional[int]]:
    """Parse aggregator string, returning (type, percentile) tuple"""
    if aggregator.startswith('p') and len(aggregator) > 1:
        try:
            percentile = int(aggregator[1:])
            if 0 <= percentile <= 100:
                return "percentile", percentile
        except ValueError:
            pass
    return aggregator, None


def build_granularity_stage(granularity: str) -> Optional[Dict[str, Any]]:
    """Build MongoDB aggregation stage for granularity grouping"""
    amount, unit = parse_granularity(granularity)
    if amount == 0:
        return None
    
    # Map units to MongoDB $dateTrunc units
    unit_mapping = {
        "s": "second",
        "m": "minute", 
        "h": "hour",
        "d": "day",
        "w": "week",
        "M": "month",
        "y": "year"
    }
    
    if unit not in unit_mapping:
        raise ValueError(f"Unsupported time unit: {unit}")
    
    return {
        "$addFields": {
            "points.bucket": {
                "$dateTrunc": {
                    "date": "$points.x",
                    "unit": unit_mapping[unit],
                    "binSize": amount
                }
            }
        }
    }


def build_aggregator_stage(aggregator: str) -> Dict[str, Any]:
    """Build MongoDB aggregation stage for value aggregation"""
    agg_type, percentile = parse_aggregator(aggregator)
    
    aggregation_map = {
        "first": {"$first": "$points.y"},
        "last": {"$last": "$points.y"}, 
        "sum": {"$sum": "$points.y"},
        "avg": {"$avg": "$points.y"},
        "median": {"$median": {"input": "$points.y", "method": "approximate"}},
        "max": {"$max": "$points.y"},
        "min": {"$min": "$points.y"},
        "count": {"$sum": 1}
    }
    
    if agg_type == "percentile":
        return {
            "$group": {
                "_id": "$points.bucket",
                "value": {
                    "$percentile": {
                        "input": "$points.y",
                        "p": [percentile / 100.0],
                        "method": "approximate"
                    }
                },
                "timestamp": {"$first": "$points.bucket"}
            }
        }
    elif agg_type in aggregation_map:
        return {
            "$group": {
                "_id": "$points.bucket", 
                "value": aggregation_map[agg_type],
                "timestamp": {"$first": "$points.bucket"}
            }
        }
    else:
        raise ValueError(f"Unknown aggregator: {aggregator}")


async def increment_miss_counter(indicator_id: str, granularity: str, aggregator: str) -> int:
    """Increment miss counter and return new count"""
    counter_key = get_counter_key(indicator_id, granularity, aggregator)
    
    try:
        count = await redis_client.incr(counter_key)
        if count == 1:
            await redis_client.expire(counter_key, settings.MISS_COUNTER_TTL)
        return count
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Redis error incrementing miss counter: {e}")
        return 0


async def cache_full_indicator(indicator_id: str, granularity: str, aggregator: str):
    """Background task to cache full indicator data"""
    logger.info(f"Caching full indicator {indicator_id} with granularity {granularity}, aggregator {aggregator}")
    
    try:
        # Build aggregation pipeline for full data
        pipeline = [{"$match": {"indicator_id": ObjectId(indicator_id)}}]
        
        # Add granularity stage if needed
        granularity_stage = build_granularity_stage(granularity)
        if granularity_stage:
            pipeline.extend([
                {"$unwind": "$points"},
                granularity_stage,
                build_aggregator_stage(aggregator),
                {"$sort": {"timestamp": 1}},
                {"$project": {
                    "_id": 0,
                    "x": "$timestamp", 
                    "y": "$value" if parse_aggregator(aggregator)[0] != "percentile" else {"$arrayElemAt": ["$value", 0]}
                }}
            ])
        else:
            pipeline.append({"$project": {"points": 1, "_id": 0}})
        
        result = await db.merged_indicators.aggregate(pipeline).to_list(None)
        
        if granularity_stage:
            # Granular data - already processed
            data_points = [{"x": r["x"].isoformat() if isinstance(r["x"], datetime) else r["x"], "y": r["y"]} for r in result]
        else:
            # Raw data - extract points
            if not result:
                logger.warning(f"No merged data found for indicator {indicator_id}")
                return
            points = result[0]["points"]
            data_points = []
            for p in points:
                if isinstance(p["x"], datetime):
                    data_points.append({"x": p["x"].isoformat(), "y": p["y"]})
                else:
                    data_points.append({"x": float(p["x"]), "y": p["y"]})

        full_cache_key = get_cache_key(indicator_id, granularity, aggregator)
        await redis_client.set(
            full_cache_key,
            json.dumps(data_points),
            ex=settings.CACHE_TTL_SECONDS
        )
        
        counter_key = get_counter_key(indicator_id, granularity, aggregator)
        await redis_client.delete(counter_key)
        
        logger.info(f"Successfully cached full indicator {indicator_id} with granularity {granularity}, aggregator {aggregator}")
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error caching full indicator {indicator_id}: {e}")
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Redis error caching full indicator {indicator_id}: {e}")


async def _get_data_points(
        indicator_id: str,
        granularity: str,
        aggregator: str,
        specific_cache_key: str,
        mongodb_pipeline: List[Dict[str, Any]],
        full_cache_processor: Optional[callable] = None,
        background_tasks: Optional[BackgroundTasks] = None
        ) -> List[DataPoint]:
    """Generic function for adaptive caching with MongoDB aggregation"""

    # Try specific cache first
    try:
        cached_data = await redis_client.get(specific_cache_key)
        if cached_data:
            cached_points = json.loads(cached_data)
            return [DataPoint(
                x=datetime.fromisoformat(p["x"]) if isinstance(p["x"], str) and 'T' in p["x"] else float(p["x"]),
                y=p["y"]
            ) for p in cached_points]
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Redis cache error: {e}")

    # Try full cache
    full_cache_key = get_cache_key(indicator_id, granularity, aggregator)
    try:
        cached_data = await redis_client.get(full_cache_key)
        if cached_data and full_cache_processor:
            # Full cache hit - process and cache the result
            cached_points = json.loads(cached_data)
            data_points = [DataPoint(
                x=datetime.fromisoformat(p["x"]) if isinstance(p["x"], str) and 'T' in p["x"] else float(p["x"]),
                y=p["y"]
            ) for p in cached_points]

            # Apply operation-specific processing
            result = full_cache_processor(data_points)

            # Cache the processed result
            try:
                cache_data = []
                for point in result:
                    if isinstance(point.x, datetime):
                        cache_data.append({"x": point.x.isoformat(), "y": point.y})
                    else:
                        cache_data.append({"x": float(point.x), "y": point.y})

                await redis_client.set(
                    specific_cache_key,
                    json.dumps(cache_data),
                    ex=settings.CACHE_TTL_SECONDS
                )
            except (RedisConnectionError, RedisTimeoutError) as e:
                logger.error(f"Redis cache error: {e}")

            return result
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Redis cache error: {e}")

    # Both caches missed - increment counter and do MongoDB query
    miss_count = await increment_miss_counter(indicator_id, granularity, aggregator)
    logger.debug(f"Cache miss for indicator {indicator_id}, granularity {granularity}, count: {miss_count}")

    try:
        # Execute MongoDB aggregation pipeline
        result = await db.merged_indicators.aggregate(mongodb_pipeline).to_list(None)

        if not result:
            logger.warning(f"No merged data found for indicator {indicator_id}")
            return []

        # Convert results to DataPoints
        data_points = []
        for r in result:
            if isinstance(r["x"], datetime):
                data_points.append(DataPoint(x=r["x"], y=r["y"]))
            else:
                data_points.append(DataPoint(x=float(r["x"]), y=r["y"]))

        # Cache the result
        try:
            cache_data = []
            for point in data_points:
                if isinstance(point.x, datetime):
                    cache_data.append({"x": point.x.isoformat(), "y": point.y})
                else:
                    cache_data.append({"x": float(point.x), "y": point.y})

            await redis_client.set(
                specific_cache_key,
                json.dumps(cache_data),
                ex=settings.CACHE_TTL_SECONDS
            )

            # Check if we should trigger full cache
            if miss_count >= settings.MISS_THRESHOLD and background_tasks:
                logger.info(f"Miss threshold reached for indicator {indicator_id}, scheduling full cache")
                background_tasks.add_task(cache_full_indicator, indicator_id, granularity, aggregator)

        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.error(f"Redis cache error: {e}")

        return data_points

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error: {e}")
        raise


def _build_query_pipeline(
    indicator_id: str,
    granularity: str, 
    aggregator: str,
    sort: str = "asc",
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Build common MongoDB aggregation pipeline"""
    pipeline = [{"$match": {"indicator_id": ObjectId(indicator_id)}}, {"$unwind": "$points"}]
    
    # Add date filtering if provided
    if start_date or end_date:
        date_match = {}
        if start_date:
            date_match["$gte"] = start_date
        if end_date:
            date_match["$lte"] = end_date
        pipeline.append({"$match": {"points.x": date_match}})
    
    # Handle granularity aggregation
    granularity_stage = build_granularity_stage(granularity)
    if granularity_stage:
        pipeline.extend([
            granularity_stage,
            build_aggregator_stage(aggregator)
        ])
        sort_field = "timestamp"
        x_field = "$timestamp"
        y_field = "$value" if parse_aggregator(aggregator)[0] != "percentile" else {"$arrayElemAt": ["$value", 0]}
    else:
        sort_field = "points.x"
        x_field = "$points.x"
        y_field = "$points.y"
    
    # Common sorting, pagination and projection
    pipeline.append({"$sort": {sort_field: -1 if sort == "desc" else 1}})
    if skip > 0:
        pipeline.append({"$skip": skip})
    pipeline.extend([
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "x": x_field,
            "y": y_field
        }}
    ])
    
    return pipeline


def _build_cache_processor(
    sort: str = "asc",
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> callable:
    """Build cache processor for full cache filtering"""
    def process_cache(data_points: List[DataPoint]) -> List[DataPoint]:
        # Filter by date if needed
        if start_date or end_date:
            data_points = [
                p for p in data_points if 
                (not start_date or (isinstance(p.x, datetime) and p.x >= start_date)) and
                (not end_date or (isinstance(p.x, datetime) and p.x <= end_date))
            ]
        
        # Sort
        data_points = sorted(data_points, key=lambda p: p.x, reverse=(sort == "desc"))
        
        # Apply pagination
        return data_points[skip:skip + limit]
    
    return process_cache


async def get_data_points(
        indicator_id: str,
        skip: int = 0,
        limit: int = 100,
        sort: str = "asc",
        granularity: str = "0",
        aggregator: str = "last",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        background_tasks: Optional[BackgroundTasks] = None
        ) -> List[DataPoint]:
    """Get data points with optional filtering, pagination and aggregation"""
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be before end_date")

    if granularity == "auto":
        granularity = await resolve_auto_granularity(indicator_id, limit)

    # Build cache key with all relevant parameters
    cache_params = {"skip": skip, "limit": limit, "sort": sort}
    if start_date:
        cache_params["start_date"] = start_date.isoformat()
    if end_date:
        cache_params["end_date"] = end_date.isoformat()

    cache_key = get_cache_key(indicator_id, granularity, aggregator, **cache_params)
    pipeline = _build_query_pipeline(indicator_id, granularity, aggregator, sort, skip, limit, start_date, end_date)
    cache_processor = _build_cache_processor(sort, skip, limit, start_date, end_date)

    return await _get_data_points(
        indicator_id=indicator_id,
        granularity=granularity,
        aggregator=aggregator,
        specific_cache_key=cache_key,
        mongodb_pipeline=pipeline,
        full_cache_processor=cache_processor,
        background_tasks=background_tasks
    )


async def _merge_resource_segments(indicator_id: str, resource_id) -> List[DataPoint]:
    """Merge all segments for a single (indicator, resource) into sorted points.

    Dedup key is (x, series) — same rule as merge_indicator_data. Distinct
    series within a resource stay distinct (one chart line each); legacy
    series-less points share the (x, None) key.
    """
    segments = await db.data_segments.find({
        "indicator_id": ObjectId(indicator_id),
        "resource_id": resource_id,
    }).to_list(None)

    time_points: Dict[tuple, tuple] = {}
    numeric_points: List[DataPoint] = []

    for segment in segments:
        seg_ts = segment.get("timestamp")
        if not seg_ts:
            continue
        for point in segment.get("points", []):
            x_raw = point.get("x")
            series = point.get("series")
            if isinstance(x_raw, str):
                try:
                    x_val = datetime.fromisoformat(x_raw.replace("Z", "+00:00"))
                except ValueError:
                    continue
            else:
                x_val = x_raw

            if isinstance(x_val, datetime):
                key = (x_val, series)
                cur = time_points.get(key)
                if not cur or seg_ts > cur[1]:
                    time_points[key] = (point["y"], seg_ts)
            else:
                try:
                    numeric_points.append(DataPoint(x=float(x_val), y=float(point["y"]), series=series))
                except (TypeError, ValueError):
                    continue

    pts = [DataPoint(x=x, y=y, series=series) for (x, series), (y, _) in time_points.items()] + numeric_points
    return sorted(pts, key=lambda p: p.x)


async def get_series_data_points(
        indicator_id: str,
        sort: str = "asc",
        skip: int = 0,
        limit: int = 1000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """One chart line per (resource_id, series_label) pair.

    Returns: [{"resource_id": str, "series_label": str|None,
               "points": [{"x": iso|float, "y": float}, ...]}, ...]

    A single XLSX with N value columns lives in ONE resource but produces N
    series here — one entry per column. Resources with no series labels
    produce a single entry with series_label=None (legacy behaviour).
    """
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be before end_date")

    resource_ids = await db.data_segments.distinct(
        "resource_id",
        {"indicator_id": ObjectId(indicator_id)},
    )

    series_list: List[Dict[str, Any]] = []
    for rid in resource_ids:
        points = await _merge_resource_segments(indicator_id, rid)

        if start_date or end_date:
            points = [
                p for p in points
                if isinstance(p.x, datetime)
                and (not start_date or p.x >= start_date)
                and (not end_date or p.x <= end_date)
            ]

        # Group by series label inside this resource.
        by_series: Dict[Optional[str], List[DataPoint]] = {}
        for p in points:
            by_series.setdefault(p.series, []).append(p)

        for series_label, group in by_series.items():
            group.sort(key=lambda p: p.x, reverse=(sort == "desc"))
            paged = group[skip:skip + limit]
            series_list.append({
                "resource_id": str(rid),
                "series_label": series_label,
                "points": [
                    {
                        "x": p.x.isoformat() if isinstance(p.x, datetime) else float(p.x),
                        "y": p.y,
                    }
                    for p in paged
                ],
            })

    return series_list


