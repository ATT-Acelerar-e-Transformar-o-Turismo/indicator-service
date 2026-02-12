import json
import logging
from datetime import datetime

import aio_pika
from bson.objectid import ObjectId

from config import settings
from dependencies.database import db
from dependencies.rabbitmq import consumer
from dependencies.redis import redis_client
from services.data_ingestor import delete_keys_by_prefix, merge_indicator_data
from services.data_propagator import get_cache_key, get_counter_key
from services.statistics_service import STATS_CACHE_PREFIX


logger = logging.getLogger(__name__)


def _serialize_merged_points(points):
    serialized = []
    for point in points:
        if isinstance(point, dict):
            serialized.append(point)
            continue

        if hasattr(point, "model_dump"):
            serialized.append(point.model_dump())
            continue

        serialized.append({"x": point.x, "y": point.y})
    return serialized


@consumer(settings.RESOURCE_DELETED_QUEUE)
async def process_resource_deleted(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            resource_id = payload.get("resource_id")
            if not resource_id:
                logger.warning("Invalid resource deletion message: missing resource_id")
                return

            indicators = await db.indicators.find(
                {"resources": resource_id, "deleted": False}
            ).to_list(length=None)

            if not indicators:
                logger.info(f"No indicators linked to resource {resource_id}")
                return

            resource_object_id = None
            try:
                resource_object_id = ObjectId(resource_id)
            except Exception:
                logger.warning(
                    f"Resource ID {resource_id} is not a valid ObjectId; skipping data_segments deletion"
                )

            for indicator in indicators:
                indicator_id = indicator["_id"]
                indicator_id_str = str(indicator_id)

                try:
                    await db.indicators.update_one(
                        {"_id": indicator_id},
                        {"$pull": {"resources": resource_id}},
                    )

                    if resource_object_id is not None:
                        await db.data_segments.delete_many(
                            {
                                "indicator_id": indicator_id,
                                "resource_id": resource_object_id,
                            }
                        )

                    merged_points = await merge_indicator_data(indicator_id_str)
                    await db.merged_indicators.update_one(
                        {"indicator_id": indicator_id},
                        {
                            "$set": {
                                "points": _serialize_merged_points(merged_points),
                                "last_updated": datetime.utcnow(),
                            }
                        },
                        upsert=True,
                    )

                    data_cache_prefix = get_cache_key(indicator_id_str, "")
                    await delete_keys_by_prefix(redis_client, data_cache_prefix)

                    counter_prefix = get_counter_key(indicator_id_str, "")
                    await delete_keys_by_prefix(redis_client, counter_prefix)

                    stats_cache_prefix = f"{STATS_CACHE_PREFIX}{indicator_id_str}"
                    await delete_keys_by_prefix(redis_client, stats_cache_prefix)

                    logger.info(
                        f"Cleaned indicator {indicator_id_str} after resource deletion {resource_id}"
                    )
                except Exception as indicator_error:
                    logger.error(
                        f"Failed cleanup for indicator {indicator_id_str} and resource {resource_id}: {indicator_error}",
                        exc_info=True,
                    )

        except json.JSONDecodeError:
            logger.error("Invalid JSON format in resource deletion message")
        except Exception as e:
            logger.error(
                f"Failed to process resource deletion message: {e}", exc_info=True
            )
