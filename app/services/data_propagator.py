from typing import List, Union, Dict, Optional, Any
from datetime import datetime, timedelta
import json
import re
import time
from schemas.data_segment import DataPoint
from dependencies.database import db
from dependencies.redis import redis_client
from bson.objectid import ObjectId
from bson.errors import InvalidId
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


def _merge_segments(segments: list) -> List[DataPoint]:
    """Merge a pre-fetched list of segments for one (indicator, resource) into sorted points.

    Dedup key is (x, series) — same rule as merge_indicator_data. Distinct
    series within a resource stay distinct (one chart line each); legacy
    series-less points share the (x, None) key.
    """
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
                    x_val = _to_naive_utc(datetime.fromisoformat(x_raw.replace("Z", "+00:00")))
                except ValueError:
                    continue
            else:
                x_val = _to_naive_utc(x_raw) if isinstance(x_raw, datetime) else x_raw

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


def _to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """MongoDB returns naive UTC datetimes; FastAPI parses query strings
    like `1993-01-01T00:00:00.000Z` as tz-aware. Comparing the two raises
    TypeError ("can't compare offset-naive and offset-aware datetimes"),
    so normalise everything to naive UTC before comparing.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    from datetime import timezone
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


_COMPOSED_DEPTH_CAP = 8  # belt-and-braces; cycles are also rejected at write time


_SECS_BY_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _bucket_start(dt: datetime, amount: int, unit: str) -> datetime:
    """Bucket-start datetime for `dt` given (amount, unit).

    Sub-month units use epoch-second truncation so any datetime maps to a
    deterministic bucket. Month/year units use calendar truncation so labels
    land on natural boundaries (Jan 1, month 1).
    """
    if unit in _SECS_BY_UNIT:
        bucket_secs = amount * _SECS_BY_UNIT[unit]
        # Compute epoch seconds for a naive UTC datetime without tz conversion.
        epoch = int((dt - datetime(1970, 1, 1)).total_seconds())
        truncated = (epoch // bucket_secs) * bucket_secs
        return datetime(1970, 1, 1) + timedelta(seconds=truncated)
    if unit == "M":
        idx = (dt.month - 1)
        bucket_idx = (idx // amount) * amount
        return datetime(dt.year, bucket_idx + 1, 1)
    if unit == "y":
        y = (dt.year // amount) * amount if amount > 1 else dt.year
        return datetime(y, 1, 1)
    raise ValueError(f"Unsupported unit: {unit}")


def _aggregate_bucket(ys: List[float], aggregator: str) -> float:
    agg_type, percentile = parse_aggregator(aggregator)
    if not ys:
        return 0.0
    if agg_type == "avg":
        return sum(ys) / len(ys)
    if agg_type == "sum":
        return sum(ys)
    if agg_type == "max":
        return max(ys)
    if agg_type == "min":
        return min(ys)
    if agg_type == "first":
        return ys[0]
    if agg_type == "last":
        return ys[-1]
    if agg_type == "count":
        return float(len(ys))
    if agg_type == "median":
        s = sorted(ys)
        n = len(s)
        return (s[n // 2] + s[(n - 1) // 2]) / 2
    if agg_type == "percentile":
        s = sorted(ys)
        n = len(s)
        idx = max(0, min(n - 1, int((percentile or 0) / 100.0 * (n - 1))))
        return s[idx]
    raise ValueError(f"Unknown aggregator: {aggregator}")


def _bucket_points(
    points: List[DataPoint],
    amount: int,
    unit: str,
    aggregator: str,
) -> List[DataPoint]:
    """Downsample a per-series point list into time buckets. Non-datetime
    (legacy numeric) points pass through unmodified — bucketing only makes
    sense on a time axis."""
    buckets: Dict[datetime, List[float]] = {}
    numeric: List[DataPoint] = []
    series_label = points[0].series if points else None
    for p in points:
        if isinstance(p.x, datetime):
            x = _to_naive_utc(p.x)
            start = _bucket_start(x, amount, unit)
            buckets.setdefault(start, []).append(p.y)
        else:
            numeric.append(p)
    aggregated = [
        DataPoint(x=k, y=_aggregate_bucket(buckets[k], aggregator), series=series_label)
        for k in sorted(buckets.keys())
    ]
    return aggregated + numeric


def _resolve_auto_granularity_from_series(
    raw_series: List[Dict[str, Any]],
    target: int,
) -> str:
    """Pick a bucket size so the longest series in `raw_series` fits in
    `target` points. Returns "0" (no bucketing) if the data is already small
    enough or has no datetime span."""
    if target <= 0 or not raw_series:
        return "0"

    min_x: Optional[datetime] = None
    max_x: Optional[datetime] = None
    max_n = 0
    for s in raw_series:
        pts = s.get("points") or []
        n = sum(1 for p in pts if isinstance(p.x, datetime))
        if n > max_n:
            max_n = n
        for p in pts:
            if not isinstance(p.x, datetime):
                continue
            x = _to_naive_utc(p.x)
            if min_x is None or x < min_x:
                min_x = x
            if max_x is None or x > max_x:
                max_x = x

    if max_n <= target or min_x is None or max_x is None:
        return "0"

    span_seconds = max(1.0, (max_x - min_x).total_seconds())
    bucket_seconds = span_seconds / target
    chosen = _AUTO_BUCKETS[-1]
    for amount, unit, secs in _AUTO_BUCKETS:
        if secs >= bucket_seconds:
            chosen = (amount, unit, secs)
            break
    return f"{chosen[0]}{chosen[1]}"


async def _own_series_for_indicator(
    indicator_id: str,
    sd: Optional[datetime],
    ed: Optional[datetime],
    source_indicator_id: str,
    source_indicator_name: str,
    source_indicator_name_en: str,
) -> List[Dict[str, Any]]:
    """Build the (resource_id, series_label) → raw-points list for ONE
    indicator's own data segments. Date-filtered but otherwise untouched —
    bucketing, sort, skip/limit, and serialisation happen at the top level
    in `get_series_data_points` so auto granularity can see the full span
    across the whole composed tree.
    """
    all_segments = await db.data_segments.find(
        {"indicator_id": ObjectId(indicator_id)},
    ).to_list(None)

    segments_by_resource: Dict[Any, list] = {}
    for seg in all_segments:
        rid = seg.get("resource_id")
        segments_by_resource.setdefault(rid, []).append(seg)

    series_list: List[Dict[str, Any]] = []
    for rid, segments in segments_by_resource.items():
        points = _merge_segments(segments)

        if sd or ed:
            def _in_range(p):
                if not isinstance(p.x, datetime):
                    return True
                x = _to_naive_utc(p.x)
                return (not sd or x >= sd) and (not ed or x <= ed)
            points = [p for p in points if _in_range(p)]

        by_series: Dict[Optional[str], List[DataPoint]] = {}
        for p in points:
            by_series.setdefault(p.series, []).append(p)

        for series_label, group in by_series.items():
            series_list.append({
                "resource_id": str(rid),
                "series_label": series_label,
                "points": group,  # raw DataPoint objects, finalised at top level
                "source_indicator_id": source_indicator_id,
                "source_indicator_name": source_indicator_name,
                "source_indicator_name_en": source_indicator_name_en,
            })

    return series_list


async def get_series_data_points(
        indicator_id: str,
        sort: str = "asc",
        skip: int = 0,
        limit: int = 1000,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        granularity: str = "0",
        aggregator: str = "avg",
) -> List[Dict[str, Any]]:
    """One chart line per (source_indicator_id, resource_id, series_label).

    For a plain indicator this is one entry per (resource_id, series_label),
    with source_indicator_* identifying the requested indicator.

    For a composed indicator (one with `child_indicators` populated) the
    response also includes every child's own series, walked transitively
    with cycle protection — equivalent to inlining each child's chart into
    the parent.
    """
    sd = _to_naive_utc(start_date)
    ed = _to_naive_utc(end_date)
    if sd and ed and sd > ed:
        raise ValueError("start_date must be before end_date")

    if not re.match(r"^(last|first|sum|avg|median|max|min|count|p([0-9]|[1-9][0-9]|100))$", aggregator):
        raise ValueError(f"Invalid aggregator: {aggregator}")

    raw_series: List[Dict[str, Any]] = []
    visited: set = set()

    async def _walk(current_id: str, depth: int) -> None:
        if depth > _COMPOSED_DEPTH_CAP:
            return
        if current_id in visited:
            return
        visited.add(current_id)
        try:
            current_oid = ObjectId(current_id)
        except (InvalidId, TypeError, ValueError):
            return
        indicator_doc = await db.indicators.find_one(
            {"_id": current_oid, "deleted": False}
        )
        if not indicator_doc:
            return

        own = await _own_series_for_indicator(
            current_id,
            sd, ed,
            source_indicator_id=current_id,
            source_indicator_name=indicator_doc.get("name") or "",
            source_indicator_name_en=indicator_doc.get("name_en") or "",
        )
        raw_series.extend(own)

        for child_id in indicator_doc.get("child_indicators", []) or []:
            await _walk(str(child_id), depth + 1)

    await _walk(str(indicator_id), 0)

    # Resolve auto granularity from the in-memory raw data so the picked
    # bucket spans the whole composed tree, not just one branch. Then bucket
    # each series independently; numeric (non-datetime) points pass through.
    resolved_granularity = granularity
    if granularity == "auto":
        resolved_granularity = _resolve_auto_granularity_from_series(raw_series, limit)

    if resolved_granularity not in ("0", "", None):
        amount, unit = parse_granularity(resolved_granularity)
        if amount > 0:
            for s in raw_series:
                s["points"] = _bucket_points(s["points"], amount, unit, aggregator)

    def _point_sort_key(p):
        if isinstance(p.x, datetime):
            return (0, _to_naive_utc(p.x), 0.0)
        return (1, datetime.min, float(p.x))

    series_list: List[Dict[str, Any]] = []
    for s in raw_series:
        pts: List[DataPoint] = s["points"]
        pts.sort(key=_point_sort_key, reverse=(sort == "desc"))
        paged = pts[skip:skip + limit]
        series_list.append({
            "resource_id": s["resource_id"],
            "series_label": s["series_label"],
            "points": [
                {
                    "x": p.x.isoformat() if isinstance(p.x, datetime) else float(p.x),
                    "y": p.y,
                }
                for p in paged
            ],
            "source_indicator_id": s["source_indicator_id"],
            "source_indicator_name": s["source_indicator_name"],
            "source_indicator_name_en": s["source_indicator_name_en"],
        })

    return series_list


