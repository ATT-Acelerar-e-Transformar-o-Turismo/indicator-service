from typing import List, Optional
from bson.objectid import ObjectId
from dependencies.database import db
from schemas.common import PyObjectId
from schemas.indicator import IndicatorCreate, IndicatorDelete
from utils.mongo_utils import serialize, deserialize

async def create_indicator(domain_id: str, subdomain_name: str, indicator_data: IndicatorCreate) -> Optional[dict]:
    indicator_dict = deserialize(indicator_data.dict())
    indicator_dict["_id"] = ObjectId()
    domain = await db.domains.find_one({"_id": ObjectId(domain_id)})
    if not domain:
        raise ValueError("Domain not found")
    if subdomain_name not in domain.get("subdomains", []):
        raise ValueError("Subdomain not found")
    indicator_dict["subdomain"] = subdomain_name
    indicator_dict["domain"] = serialize(domain)
    indicator_dict["deleted"] = False
    result = await db.indicators.insert_one(indicator_dict)
    if not result.inserted_id:
        raise ValueError("Failed to create indicator")
    return serialize(indicator_dict)

async def get_all_indicators(skip: int = 0, limit: int = 10) -> List[dict]:
    indicators = await db.indicators.find({"deleted": False}).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]

async def get_indicator_by_id(indicator_id: str) -> Optional[dict]:
    indicator = await db.indicators.find_one({"_id": ObjectId(indicator_id), "deleted": False})
    if not indicator:
        return None
    domain_id = indicator["domain"]["_id"] if isinstance(indicator["domain"], dict) else indicator["domain"]
    domain = await db.domains.find_one({"_id": domain_id, "deleted": False})
    if not domain:
        return None
    indicator["domain"] = serialize(domain)
    return serialize(indicator)

async def update_indicator(indicator_id: str, update_data: dict) -> int:
    if "domain" in update_data:
        domain_id = update_data["domain"]
        domain = await db.domains.find_one({"_id": ObjectId(domain_id)})
        if not domain:
            raise ValueError("Domain not found")
        if "subdomain" in update_data and update_data["subdomain"] not in domain.get("subdomains", []):
            raise ValueError("Subdomain not found")
        update_data["domain"] = serialize(domain)

    result = await db.indicators.update_one(
        {"_id": ObjectId(indicator_id), "deleted": False},
        {"$set": deserialize(update_data)}
    )
    return result.modified_count

async def delete_indicator(indicator_id: str) -> Optional[IndicatorDelete]:
    result = await db.indicators.update_one(
        {"_id": ObjectId(indicator_id)},
        {"$set": {"deleted": True}}
    )
    if result.modified_count > 0:
        return IndicatorDelete(id=indicator_id, deleted=True)
    return None

async def get_indicators_by_domain(domain_id: str, skip: int = 0, limit: int = 10) -> List[dict]:
    indicators = await db.indicators.find(
        {"domain._id": ObjectId(domain_id), "deleted": False}
    ).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]

async def get_indicators_by_subdomain(domain_id: str, subdomain_name: str, skip: int = 0, limit: int = 10) -> List[dict]:
    indicators = await db.indicators.find(
        {"domain._id": ObjectId(domain_id), "subdomain": subdomain_name, "deleted": False}
    ).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]