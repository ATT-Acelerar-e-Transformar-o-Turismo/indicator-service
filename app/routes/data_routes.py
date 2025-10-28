from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Response
from typing import List, Optional
from datetime import datetime
import re
from schemas.data_segment import DataPoint
from schemas.common import PyObjectId
from bson.errors import InvalidId
from services.data_propagator import get_data_points
from services.statistics_service import get_indicator_statistics
from schemas.statistics import IndicatorStatistics
from config import settings
from dependencies.database import db
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


@router.post("/fake/insert-csv-data/{indicator_id}")
async def fake_insert_csv_data(indicator_id: str):
    """Fake endpoint to insert hardcoded housing price data until 2020 (for CSV/XLSX demo)"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    # Hardcoded housing price data until 2020
    data_until_2020 = [{"x": "2011-01-01T00:00:00", "y": 877.0}, {"x": "2011-02-01T00:00:00", "y": 887.0}, {"x": "2011-03-01T00:00:00", "y": 898.0}, {"x": "2011-04-01T00:00:00", "y": 900.0}, {"x": "2011-05-01T00:00:00", "y": 889.0}, {"x": "2011-06-01T00:00:00", "y": 872.0}, {"x": "2011-07-01T00:00:00", "y": 859.0}, {"x": "2011-08-01T00:00:00", "y": 852.0}, {"x": "2011-09-01T00:00:00", "y": 844.0}, {"x": "2011-10-01T00:00:00", "y": 833.0}, {"x": "2011-11-01T00:00:00", "y": 829.0}, {"x": "2011-12-01T00:00:00", "y": 818.0}, {"x": "2012-01-01T00:00:00", "y": 809.0}, {"x": "2012-02-01T00:00:00", "y": 810.0}, {"x": "2012-03-01T00:00:00", "y": 811.0}, {"x": "2012-04-01T00:00:00", "y": 814.0}, {"x": "2012-05-01T00:00:00", "y": 800.0}, {"x": "2012-06-01T00:00:00", "y": 791.0}, {"x": "2012-07-01T00:00:00", "y": 787.0}, {"x": "2012-08-01T00:00:00", "y": 784.0}, {"x": "2012-09-01T00:00:00", "y": 784.0}, {"x": "2012-10-01T00:00:00", "y": 778.0}, {"x": "2012-11-01T00:00:00", "y": 774.0}, {"x": "2012-12-01T00:00:00", "y": 770.0}, {"x": "2013-01-01T00:00:00", "y": 759.0}, {"x": "2013-02-01T00:00:00", "y": 746.0}, {"x": "2013-03-01T00:00:00", "y": 742.0}, {"x": "2013-04-01T00:00:00", "y": 748.0}, {"x": "2013-05-01T00:00:00", "y": 758.0}, {"x": "2013-06-01T00:00:00", "y": 764.0}, {"x": "2013-07-01T00:00:00", "y": 759.0}, {"x": "2013-08-01T00:00:00", "y": 761.0}, {"x": "2013-09-01T00:00:00", "y": 763.0}, {"x": "2013-10-01T00:00:00", "y": 765.0}, {"x": "2013-11-01T00:00:00", "y": 774.0}, {"x": "2013-12-01T00:00:00", "y": 763.0}, {"x": "2014-01-01T00:00:00", "y": 760.0}, {"x": "2014-02-01T00:00:00", "y": 752.0}, {"x": "2014-03-01T00:00:00", "y": 739.0}, {"x": "2014-04-01T00:00:00", "y": 736.0}, {"x": "2014-05-01T00:00:00", "y": 738.0}, {"x": "2014-06-01T00:00:00", "y": 748.0}, {"x": "2014-07-01T00:00:00", "y": 763.0}, {"x": "2014-08-01T00:00:00", "y": 775.0}, {"x": "2014-09-01T00:00:00", "y": 772.0}, {"x": "2014-10-01T00:00:00", "y": 756.0}, {"x": "2014-11-01T00:00:00", "y": 751.0}, {"x": "2014-12-01T00:00:00", "y": 751.0}, {"x": "2015-01-01T00:00:00", "y": 756.0}, {"x": "2015-02-01T00:00:00", "y": 756.0}, {"x": "2015-03-01T00:00:00", "y": 756.0}, {"x": "2015-04-01T00:00:00", "y": 760.0}, {"x": "2015-05-01T00:00:00", "y": 768.0}, {"x": "2015-06-01T00:00:00", "y": 769.0}, {"x": "2015-07-01T00:00:00", "y": 776.0}, {"x": "2015-08-01T00:00:00", "y": 774.0}, {"x": "2015-09-01T00:00:00", "y": 777.0}, {"x": "2015-10-01T00:00:00", "y": 780.0}, {"x": "2015-11-01T00:00:00", "y": 783.0}, {"x": "2015-12-01T00:00:00", "y": 788.0}, {"x": "2016-01-01T00:00:00", "y": 785.0}, {"x": "2016-02-01T00:00:00", "y": 785.0}, {"x": "2016-03-01T00:00:00", "y": 788.0}, {"x": "2016-04-01T00:00:00", "y": 794.0}, {"x": "2016-05-01T00:00:00", "y": 800.0}, {"x": "2016-06-01T00:00:00", "y": 803.0}, {"x": "2016-07-01T00:00:00", "y": 806.0}, {"x": "2016-08-01T00:00:00", "y": 814.0}, {"x": "2016-09-01T00:00:00", "y": 816.0}, {"x": "2016-10-01T00:00:00", "y": 820.0}, {"x": "2016-11-01T00:00:00", "y": 826.0}, {"x": "2016-12-01T00:00:00", "y": 833.0}, {"x": "2017-01-01T00:00:00", "y": 841.0}, {"x": "2017-02-01T00:00:00", "y": 847.0}, {"x": "2017-03-01T00:00:00", "y": 849.0}, {"x": "2017-04-01T00:00:00", "y": 854.0}, {"x": "2017-05-01T00:00:00", "y": 857.0}, {"x": "2017-06-01T00:00:00", "y": 860.0}, {"x": "2017-07-01T00:00:00", "y": 861.0}, {"x": "2017-08-01T00:00:00", "y": 868.0}, {"x": "2017-09-01T00:00:00", "y": 881.0}, {"x": "2017-10-01T00:00:00", "y": 886.0}, {"x": "2017-11-01T00:00:00", "y": 889.0}, {"x": "2017-12-01T00:00:00", "y": 891.0}, {"x": "2018-01-01T00:00:00", "y": 902.0}, {"x": "2018-02-01T00:00:00", "y": 911.0}, {"x": "2018-03-01T00:00:00", "y": 920.0}, {"x": "2018-04-01T00:00:00", "y": 924.0}, {"x": "2018-05-01T00:00:00", "y": 929.0}, {"x": "2018-06-01T00:00:00", "y": 932.0}, {"x": "2018-07-01T00:00:00", "y": 941.0}, {"x": "2018-08-01T00:00:00", "y": 951.0}, {"x": "2018-09-01T00:00:00", "y": 959.0}, {"x": "2018-10-01T00:00:00", "y": 967.0}, {"x": "2018-11-01T00:00:00", "y": 968.0}, {"x": "2018-12-01T00:00:00", "y": 977.0}, {"x": "2019-01-01T00:00:00", "y": 983.0}, {"x": "2019-02-01T00:00:00", "y": 1000.0}, {"x": "2019-03-01T00:00:00", "y": 1006.0}, {"x": "2019-04-01T00:00:00", "y": 1015.0}, {"x": "2019-05-01T00:00:00", "y": 1023.0}, {"x": "2019-06-01T00:00:00", "y": 1030.0}, {"x": "2019-07-01T00:00:00", "y": 1044.0}, {"x": "2019-08-01T00:00:00", "y": 1054.0}, {"x": "2019-09-01T00:00:00", "y": 1066.0}, {"x": "2019-10-01T00:00:00", "y": 1069.0}, {"x": "2019-11-01T00:00:00", "y": 1076.0}, {"x": "2019-12-01T00:00:00", "y": 1091.0}, {"x": "2020-01-01T00:00:00", "y": 1103.0}, {"x": "2020-02-01T00:00:00", "y": 1111.0}, {"x": "2020-03-01T00:00:00", "y": 1110.0}, {"x": "2020-04-01T00:00:00", "y": 1111.0}, {"x": "2020-05-01T00:00:00", "y": 1114.0}, {"x": "2020-06-01T00:00:00", "y": 1119.0}, {"x": "2020-07-01T00:00:00", "y": 1127.0}, {"x": "2020-08-01T00:00:00", "y": 1128.0}, {"x": "2020-09-01T00:00:00", "y": 1128.0}, {"x": "2020-10-01T00:00:00", "y": 1131.0}, {"x": "2020-11-01T00:00:00", "y": 1144.0}, {"x": "2020-12-01T00:00:00", "y": 1156.0}]

    # Convert to MongoDB data format
    data_documents = []
    for point in data_until_2020:
        data_documents.append({
            "indicator_id": PyObjectId(indicator_id),
            "timestamp": datetime.fromisoformat(point["x"]),
            "value": point["y"],
            "created_at": datetime.utcnow()
        })

    # Insert into MongoDB
    result = await db.data_points.insert_many(data_documents)
    logger.info(f"Inserted {len(result.inserted_ids)} CSV data points for indicator {indicator_id}")

    # Also create merged_indicators entry for immediate visibility
    merged_points = [{"x": datetime.fromisoformat(p["x"]), "y": p["y"]} for p in data_until_2020]
    await db.merged_indicators.update_one(
        {"indicator_id": PyObjectId(indicator_id)},
        {"$set": {
            "last_updated": datetime.utcnow(),
            "points": merged_points
        }},
        upsert=True
    )

    return {
        "message": "CSV data inserted successfully",
        "indicator_id": indicator_id,
        "points_inserted": len(result.inserted_ids),
        "date_range": "2011-2020"
    }


@router.post("/fake/insert-api-data/{indicator_id}")
async def fake_insert_api_data(indicator_id: str):
    """Fake endpoint to insert hardcoded housing price data after 2020 (for API demo)"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    # Hardcoded housing price data after 2020
    data_after_2020 = [{"x": "2021-01-01T00:00:00", "y": 1170.0}, {"x": "2021-02-01T00:00:00", "y": 1174.0}, {"x": "2021-03-01T00:00:00", "y": 1187.0}, {"x": "2021-04-01T00:00:00", "y": 1200.0}, {"x": "2021-05-01T00:00:00", "y": 1212.0}, {"x": "2021-06-01T00:00:00", "y": 1215.0}, {"x": "2021-07-01T00:00:00", "y": 1221.0}, {"x": "2021-08-01T00:00:00", "y": 1221.0}, {"x": "2021-09-01T00:00:00", "y": 1236.0}, {"x": "2021-10-01T00:00:00", "y": 1251.0}, {"x": "2021-11-01T00:00:00", "y": 1272.0}, {"x": "2021-12-01T00:00:00", "y": 1285.0}, {"x": "2022-01-01T00:00:00", "y": 1292.0}, {"x": "2022-02-01T00:00:00", "y": 1314.0}, {"x": "2022-03-01T00:00:00", "y": 1331.0}, {"x": "2022-04-01T00:00:00", "y": 1356.0}, {"x": "2022-05-01T00:00:00", "y": 1380.0}, {"x": "2022-06-01T00:00:00", "y": 1407.0}, {"x": "2022-07-01T00:00:00", "y": 1417.0}, {"x": "2022-08-01T00:00:00", "y": 1414.0}, {"x": "2022-09-01T00:00:00", "y": 1429.0}, {"x": "2022-10-01T00:00:00", "y": 1420.0}, {"x": "2022-11-01T00:00:00", "y": 1449.0}, {"x": "2022-12-01T00:00:00", "y": 1458.0}, {"x": "2023-01-01T00:00:00", "y": 1485.0}, {"x": "2023-02-01T00:00:00", "y": 1478.0}, {"x": "2023-03-01T00:00:00", "y": 1483.0}, {"x": "2023-04-01T00:00:00", "y": 1491.0}, {"x": "2023-05-01T00:00:00", "y": 1510.0}, {"x": "2023-06-01T00:00:00", "y": 1518.0}, {"x": "2023-07-01T00:00:00", "y": 1525.0}, {"x": "2023-08-01T00:00:00", "y": 1538.0}, {"x": "2023-09-01T00:00:00", "y": 1541.0}, {"x": "2023-10-01T00:00:00", "y": 1536.0}, {"x": "2023-11-01T00:00:00", "y": 1530.0}, {"x": "2023-12-01T00:00:00", "y": 1536.0}, {"x": "2024-01-01T00:00:00", "y": 1550.0}, {"x": "2024-02-01T00:00:00", "y": 1560.0}, {"x": "2024-03-01T00:00:00", "y": 1580.0}, {"x": "2024-04-01T00:00:00", "y": 1596.0}, {"x": "2024-05-01T00:00:00", "y": 1610.0}, {"x": "2024-06-01T00:00:00", "y": 1618.0}, {"x": "2024-07-01T00:00:00", "y": 1638.0}, {"x": "2024-08-01T00:00:00", "y": 1664.0}, {"x": "2024-09-01T00:00:00", "y": 1695.0}, {"x": "2024-10-01T00:00:00", "y": 1721.0}, {"x": "2024-11-01T00:00:00", "y": 1740.0}, {"x": "2024-12-01T00:00:00", "y": 1747.0}, {"x": "2025-01-01T00:00:00", "y": 1774.0}, {"x": "2025-02-01T00:00:00", "y": 1810.0}, {"x": "2025-03-01T00:00:00", "y": 1847.0}, {"x": "2025-04-01T00:00:00", "y": 1866.0}, {"x": "2025-05-01T00:00:00", "y": 1886.0}, {"x": "2025-06-01T00:00:00", "y": 1911.0}, {"x": "2025-07-01T00:00:00", "y": 1945.0}, {"x": "2025-08-01T00:00:00", "y": 1965.0}, {"x": "2025-09-01T00:00:00", "y": 1995.0}]

    # Convert to MongoDB data format
    data_documents = []
    for point in data_after_2020:
        data_documents.append({
            "indicator_id": PyObjectId(indicator_id),
            "timestamp": datetime.fromisoformat(point["x"]),
            "value": point["y"],
            "created_at": datetime.utcnow()
        })

    # Insert into MongoDB
    result = await db.data_points.insert_many(data_documents)
    logger.info(f"Inserted {len(result.inserted_ids)} API data points for indicator {indicator_id}")

    # Append to merged_indicators (merge with existing CSV data)
    merged_points_new = [{"x": datetime.fromisoformat(p["x"]), "y": p["y"]} for p in data_after_2020]

    # Get existing data and merge
    existing = await db.merged_indicators.find_one({"indicator_id": PyObjectId(indicator_id)})
    if existing and "points" in existing:
        all_points = existing["points"] + merged_points_new
    else:
        all_points = merged_points_new

    # Sort by date
    all_points.sort(key=lambda p: p["x"])

    await db.merged_indicators.update_one(
        {"indicator_id": PyObjectId(indicator_id)},
        {"$set": {
            "last_updated": datetime.utcnow(),
            "points": all_points
        }},
        upsert=True
    )

    return {
        "message": "API data inserted successfully",
        "indicator_id": indicator_id,
        "points_inserted": len(result.inserted_ids),
        "date_range": "2021-2025"
    }
