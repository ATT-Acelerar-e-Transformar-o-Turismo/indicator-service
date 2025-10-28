from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Response
from typing import List, Optional
from datetime import datetime
import re
from schemas.data_segment import DataPoint, DataSegment, TimePoint
from services.data_ingestor import store_data_segment
from schemas.common import PyObjectId
from bson import ObjectId
from bson.errors import InvalidId
from services.data_propagator import get_data_points
from services.statistics_service import get_indicator_statistics
from schemas.statistics import IndicatorStatistics
from config import settings
from dependencies.database import db
from services.data_ingestor import delete_keys_by_prefix, merge_indicator_data, get_cache_key, get_counter_key
from dependencies.redis import redis_client
from services.statistics_service import STATS_CACHE_PREFIX
import logging

logger = logging.getLogger(__name__)

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


@router.get("/{indicator_id}/statistics", response_model=IndicatorStatistics)
async def get_indicator_stats(
    indicator_id: str
):
    """Get comprehensive statistics for an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    stats = await get_indicator_statistics(indicator_id)
    return stats


from services.data_ingestor import store_data_segment
from schemas.data_segment import DataSegment, TimePoint

@router.post("/fake/insert-csv-data/{indicator_id}")
async def fake_insert_csv_data(indicator_id: str):
    """Fake endpoint to insert hardcoded housing price data until 2020 (for CSV/XLSX demo)"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    logger.info(f"Inserting fake CSV data for indicator {indicator_id}")
    
    # Hardcoded housing price data until 2020
    data_until_2020 = [{"x": "2011-01-01T00:00:00", "y": 877.0}, {"x": "2011-02-01T00:00:00", "y": 887.0}, {"x": "2011-03-01T00:00:00", "y": 898.0}, {"x": "2011-04-01T00:00:00", "y": 900.0}, {"x": "2011-05-01T00:00:00", "y": 889.0}, {"x": "2011-06-01T00:00:00", "y": 872.0}, {"x": "2011-07-01T00:00:00", "y": 859.0}, {"x": "2011-08-01T00:00:00", "y": 852.0}, {"x": "2011-09-01T00:00:00", "y": 844.0}, {"x": "2011-10-01T00:00:00", "y": 833.0}, {"x": "2011-11-01T00:00:00", "y": 829.0}, {"x": "2011-12-01T00:00:00", "y": 818.0}, {"x": "2012-01-01T00:00:00", "y": 809.0}, {"x": "2012-02-01T00:00:00", "y": 810.0}, {"x": "2012-03-01T00:00:00", "y": 811.0}, {"x": "2012-04-01T00:00:00", "y": 814.0}, {"x": "2012-05-01T00:00:00", "y": 800.0}, {"x": "2012-06-01T00:00:00", "y": 791.0}, {"x": "2012-07-01T00:00:00", "y": 787.0}, {"x": "2012-08-01T00:00:00", "y": 784.0}, {"x": "2012-09-01T00:00:00", "y": 784.0}, {"x": "2012-10-01T00:00:00", "y": 778.0}, {"x": "2012-11-01T00:00:00", "y": 774.0}, {"x": "2012-12-01T00:00:00", "y": 770.0}, {"x": "2013-01-01T00:00:00", "y": 759.0}, {"x": "2013-02-01T00:00:00", "y": 746.0}, {"x": "2013-03-01T00:00:00", "y": 742.0}, {"x": "2013-04-01T00:00:00", "y": 748.0}, {"x": "2013-05-01T00:00:00", "y": 758.0}, {"x": "2013-06-01T00:00:00", "y": 764.0}, {"x": "2013-07-01T00:00:00", "y": 759.0}, {"x": "2013-08-01T00:00:00", "y": 761.0}, {"x": "2013-09-01T00:00:00", "y": 763.0}, {"x": "2013-10-01T00:00:00", "y": 765.0}, {"x": "2013-11-01T00:00:00", "y": 774.0}, {"x": "2013-12-01T00:00:00", "y": 763.0}, {"x": "2014-01-01T00:00:00", "y": 760.0}, {"x": "2014-02-01T00:00:00", "y": 752.0}, {"x": "2014-03-01T00:00:00", "y": 739.0}, {"x": "2014-04-01T00:00:00", "y": 736.0}, {"x": "2014-05-01T00:00:00", "y": 738.0}, {"x": "2014-06-01T00:00:00", "y": 748.0}, {"x": "2014-07-01T00:00:00", "y": 763.0}, {"x": "2014-08-01T00:00:00", "y": 775.0}, {"x": "2014-09-01T00:00:00", "y": 772.0}, {"x": "2014-10-01T00:00:00", "y": 756.0}, {"x": "2014-11-01T00:00:00", "y": 751.0}, {"x": "2014-12-01T00:00:00", "y": 752.0}, {"x": "2015-01-01T00:00:00", "y": 753.0}, {"x": "2015-02-01T00:00:00", "y": 758.0}, {"x": "2015-03-01T00:00:00", "y": 766.0}, {"x": "2015-04-01T00:00:00", "y": 778.0}, {"x": "2015-05-01T00:00:00", "y": 785.0}, {"x": "2015-06-01T00:00:00", "y": 794.0}, {"x": "2015-07-01T00:00:00", "y": 802.0}, {"x": "2015-08-01T00:00:00", "y": 810.0}, {"x": "2015-09-01T00:00:00", "y": 815.0}, {"x": "2015-10-01T00:00:00", "y": 823.0}, {"x": "2015-11-01T00:00:00", "y": 828.0}, {"x": "2015-12-01T00:00:00", "y": 835.0}, {"x": "2016-01-01T00:00:00", "y": 840.0}, {"x": "2016-02-01T00:00:00", "y": 845.0}, {"x": "2016-03-01T00:00:00", "y": 853.0}, {"x": "2016-04-01T00:00:00", "y": 864.0}, {"x": "2016-05-01T00:00:00", "y": 876.0}, {"x": "2016-06-01T00:00:00", "y": 889.0}, {"x": "2016-07-01T00:00:00", "y": 900.0}, {"x": "2016-08-01T00:00:00", "y": 908.0}, {"x": "2016-09-01T00:00:00", "y": 915.0}, {"x": "2016-10-01T00:00:00", "y": 923.0}, {"x": "2016-11-01T00:00:00", "y": 931.0}, {"x": "2016-12-01T00:00:00", "y": 938.0}, {"x": "2017-01-01T00:00:00", "y": 945.0}, {"x": "2017-02-01T00:00:00", "y": 952.0}, {"x": "2017-03-01T00:00:00", "y": 961.0}, {"x": "2017-04-01T00:00:00", "y": 972.0}, {"x": "2017-05-01T00:00:00", "y": 984.0}, {"x": "2017-06-01T00:00:00", "y": 996.0}, {"x": "2017-07-01T00:00:00", "y": 1007.0}, {"x": "2017-08-01T00:00:00", "y": 1016.0}, {"x": "2017-09-01T00:00:00", "y": 1025.0}, {"x": "2017-10-01T00:00:00", "y": 1034.0}, {"x": "2017-11-01T00:00:00", "y": 1043.0}, {"x": "2017-12-01T00:00:00", "y": 1052.0}, {"x": "2018-01-01T00:00:00", "y": 1061.0}, {"x": "2018-02-01T00:00:00", "y": 1070.0}, {"x": "2018-03-01T00:00:00", "y": 1080.0}, {"x": "2018-04-01T00:00:00", "y": 1091.0}, {"x": "2018-05-01T00:00:00", "y": 1102.0}, {"x": "2018-06-01T00:00:00", "y": 1113.0}, {"x": "2018-07-01T00:00:00", "y": 1123.0}, {"x": "2018-08-01T00:00:00", "y": 1132.0}, {"x": "2018-09-01T00:00:00", "y": 1141.0}, {"x": "2018-10-01T00:00:00", "y": 1150.0}, {"x": "2018-11-01T00:00:00", "y": 1159.0}, {"x": "2018-12-01T00:00:00", "y": 1168.0}, {"x": "2019-01-01T00:00:00", "y": 1176.0}, {"x": "2019-02-01T00:00:00", "y": 1184.0}, {"x": "2019-03-01T00:00:00", "y": 1193.0}, {"x": "2019-04-01T00:00:00", "y": 1203.0}, {"x": "2019-05-01T00:00:00", "y": 1213.0}, {"x": "2019-06-01T00:00:00", "y": 1223.0}, {"x": "2019-07-01T00:00:00", "y": 1233.0}, {"x": "2019-08-01T00:00:00", "y": 1242.0}, {"x": "2019-09-01T00:00:00", "y": 1251.0}, {"x": "2019-10-01T00:00:00", "y": 1260.0}, {"x": "2019-11-01T00:00:00", "y": 1269.0}, {"x": "2019-12-01T00:00:00", "y": 1278.0}, {"x": "2020-01-01T00:00:00", "y": 1287.0}, {"x": "2020-02-01T00:00:00", "y": 1296.0}, {"x": "2020-03-01T00:00:00", "y": 1305.0}, {"x": "2020-04-01T00:00:00", "y": 1315.0}, {"x": "2020-05-01T00:00:00", "y": 1325.0}, {"x": "2020-06-01T00:00:00", "y": 1335.0}, {"x": "2020-07-01T00:00:00", "y": 1345.0}, {"x": "2020-08-01T00:00:00", "y": 1354.0}, {"x": "2020-09-01T00:00:00", "y": 1363.0}, {"x": "2020-10-01T00:00:00", "y": 1372.0}, {"x": "2020-11-01T00:00:00", "y": 1381.0}, {"x": "2020-12-01T00:00:00", "y": 1390.0}]

    data_points = [TimePoint(x=p['x'], y=p['y']) for p in data_until_2020]

    segment = DataSegment(
        indicator_id=PyObjectId(indicator_id),
        resource_id=ObjectId(),
        points=data_points
    )

    await store_data_segment(segment)
    
    logger.info(f"Successfully inserted {len(data_points)} fake CSV data points for indicator {indicator_id}")

    return {
        "message": "CSV data inserted successfully",
        "indicator_id": indicator_id,
        "points_inserted": len(data_points),
        "date_range": "2011-2020"
    }


@router.post("/fake/insert-api-data/{indicator_id}")
async def fake_insert_api_data(indicator_id: str):
    """Fake endpoint to insert hardcoded housing price data after 2020 (for API demo)"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    logger.info(f"Inserting fake API data for indicator {indicator_id}")
    
    # Hardcoded housing price data after 2020
    data_after_2020 = [{"x": "2021-01-01T00:00:00", "y": 1170.0}, {"x": "2021-02-01T00:00:00", "y": 1174.0}, {"x": "2021-03-01T00:00:00", "y": 1187.0}, {"x": "2021-04-01T00:00:00", "y": 1200.0}, {"x": "2021-05-01T00:00:00", "y": 1212.0}, {"x": "2021-06-01T00:00:00", "y": 1215.0}, {"x": "2021-07-01T00:00:00", "y": 1221.0}, {"x": "2021-08-01T00:00:00", "y": 1221.0}, {"x": "2021-09-01T00:00:00", "y": 1236.0}, {"x": "2021-10-01T00:00:00", "y": 1251.0}, {"x": "2021-11-01T00:00:00", "y": 1272.0}, {"x": "2021-12-01T00:00:00", "y": 1285.0}, {"x": "2022-01-01T00:00:00", "y": 1292.0}, {"x": "2022-02-01T00:00:00", "y": 1314.0}, {"x": "2022-03-01T00:00:00", "y": 1331.0}, {"x": "2022-04-01T00:00:00", "y": 1356.0}, {"x": "2022-05-01T00:00:00", "y": 1380.0}, {"x": "2022-06-01T00:00:00", "y": 1407.0}, {"x": "2022-07-01T00:00:00", "y": 1417.0}, {"x": "2022-08-01T00:00:00", "y": 1414.0}, {"x": "2022-09-01T00:00:00", "y": 1429.0}, {"x": "2022-10-01T00:00:00", "y": 1420.0}, {"x": "2022-11-01T00:00:00", "y": 1449.0}, {"x": "2022-12-01T00:00:00", "y": 1458.0}, {"x": "2023-01-01T00:00:00", "y": 1485.0}, {"x": "2023-02-01T00:00:00", "y": 1478.0}, {"x": "2023-03-01T00:00:00", "y": 1483.0}, {"x": "2023-04-01T00:00:00", "y": 1491.0}, {"x": "2023-05-01T00:00:00", "y": 1510.0}, {"x": "2023-06-01T00:00:00", "y": 1518.0}, {"x": "2023-07-01T00:00:00", "y": 1525.0}, {"x": "2023-08-01T00:00:00", "y": 1538.0}, {"x": "2023-09-01T00:00:00", "y": 1541.0}, {"x": "2023-10-01T00:00:00", "y": 1536.0}, {"x": "2023-11-01T00:00:00", "y": 1530.0}, {"x": "2023-12-01T00:00:00", "y": 1536.0}, {"x": "2024-01-01T00:00:00", "y": 1550.0}, {"x": "2024-02-01T00:00:00", "y": 1560.0}, {"x": "2024-03-01T00:00:00", "y": 1580.0}, {"x": "2024-04-01T00:00:00", "y": 1596.0}, {"x": "2024-05-01T00:00:00", "y": 1610.0}, {"x": "2024-06-01T00:00:00", "y": 1618.0}, {"x": "2024-07-01T00:00:00", "y": 1638.0}, {"x": "2024-08-01T00:00:00", "y": 1664.0}, {"x": "2024-09-01T00:00:00", "y": 1695.0}, {"x": "2024-10-01T00:00:00", "y": 1721.0}, {"x": "2024-11-01T00:00:00", "y": 1748.0}, {"x": "2024-12-01T00:00:00", "y": 1778.0}, {"x": "2025-01-01T00:00:00", "y": 1800.0}]

    data_points = [TimePoint(x=p['x'], y=p['y']) for p in data_after_2020]

    segment = DataSegment(
        indicator_id=PyObjectId(indicator_id),
        resource_id=ObjectId(),
        points=data_points
    )

    await store_data_segment(segment)
    
    logger.info(f"Successfully inserted {len(data_points)} fake API data points for indicator {indicator_id}")

    return {
        "message": "API data inserted successfully",
        "indicator_id": indicator_id,
        "points_inserted": len(data_points),
        "date_range": "2021-2025"
    }
