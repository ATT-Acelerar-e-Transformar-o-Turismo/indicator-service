from typing import List, Optional
from bson.objectid import ObjectId
from dependencies.database import get_database
from utils.mongo_utils import serialize
from schemas.domain import DomainCreate, DomainUpdate, DomainPatch, DomainDelete

async def get_all_domains() -> List[dict]:
    db = get_database()
    domains = await db.domains.find({"deleted": False}).to_list(100)
    return [serialize(domain) for domain in domains]

async def get_domain_by_id(domain_id: str) -> Optional[dict]:
    db = get_database()
    domain = await db.domains.find_one({"_id": ObjectId(domain_id), "deleted": False})
    return serialize(domain) if domain else None

async def create_domain(domain_data: DomainCreate) -> Optional[dict]:
    db = get_database()
    domain_dict = domain_data.dict()
    domain_dict["deleted"] = False
    result = await db.domains.insert_one(domain_dict)
    if result.inserted_id:
        return await get_domain_by_id(str(result.inserted_id))
    return None

async def update_domain(domain_id: str, domain_data: DomainUpdate) -> Optional[dict]:
    db = get_database()
    result = await db.domains.update_one(
        {"_id": ObjectId(domain_id), "deleted": False},
        {"$set": domain_data.dict()}
    )
    if result.modified_count > 0:
        return await get_domain_by_id(domain_id)
    return None

async def patch_domain(domain_id: str, domain_data: DomainPatch) -> Optional[dict]:
    db = get_database()
    result = await db.domains.update_one(
        {"_id": ObjectId(domain_id)},
        {"$set": {**domain_data.dict(exclude_unset=True), "deleted": False}}
    )
    return await get_domain_by_id(domain_id) if result.modified_count > 0 else None

async def delete_domain(domain_id: str) -> Optional[DomainDelete]:
    db = get_database()
    domain_result = await db.domains.update_one(
        {"_id": ObjectId(domain_id)},
        {"$set": {"deleted": True}}
    )
    if domain_result.modified_count > 0:
        await db.indicators.update_many(
            {"domain": ObjectId(domain_id)},
            {"$set": {"deleted": True}}
        )
        return DomainDelete(id=domain_id, deleted=True)
    return None

