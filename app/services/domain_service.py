from typing import List, Optional
from bson.objectid import ObjectId
from dependencies.database import db
from utils.mongo_utils import serialize
from schemas.domain import DomainCreate, DomainUpdate, DomainPatch, DomainDelete

async def get_all_domains(include_hidden: bool = False) -> List[dict]:
    filter_criteria = {"deleted": False}
    if not include_hidden:
        filter_criteria["hidden"] = {"$ne": True}
    domains = await db.domains.find(filter_criteria).to_list(100)
    return [serialize(domain) for domain in domains]


async def get_hidden_domain_ids() -> List[ObjectId]:
    """Return ObjectIds of every non-deleted, hidden domain."""
    cursor = db.domains.find({"deleted": False, "hidden": True}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


async def get_hidden_dimension_keys() -> List[dict]:
    """Return {domain, subdomain} pairs for every hidden subdomain in every non-deleted domain."""
    cursor = db.domains.find({"deleted": False}, {"_id": 1, "subdomains": 1})
    keys = []
    async for doc in cursor:
        for sub in doc.get("subdomains") or []:
            if isinstance(sub, dict) and sub.get("hidden") and sub.get("name"):
                keys.append({"domain": doc["_id"], "subdomain": sub["name"]})
    return keys


async def is_subdomain_hidden(domain_id: str, subdomain_name: str) -> bool:
    """Check if a specific subdomain is marked hidden."""
    domain = await db.domains.find_one(
        {"_id": ObjectId(domain_id), "deleted": False},
        {"subdomains": 1},
    )
    if not domain:
        return False
    for sub in domain.get("subdomains") or []:
        if isinstance(sub, dict) and sub.get("name") == subdomain_name:
            return bool(sub.get("hidden"))
    return False


async def get_domain_by_id(domain_id: str) -> Optional[dict]:
    domain = await db.domains.find_one({"_id": ObjectId(domain_id), "deleted": False})
    return serialize(domain) if domain else None

async def create_domain(domain_data: DomainCreate) -> Optional[dict]:
    domain_dict = domain_data.dict()
    domain_dict["deleted"] = False
    domain_dict.setdefault("hidden", False)
    result = await db.domains.insert_one(domain_dict)
    if result.inserted_id:
        return await get_domain_by_id(str(result.inserted_id))
    return None

async def update_domain(domain_id: str, domain_data: DomainUpdate) -> Optional[dict]:
    result = await db.domains.update_one(
        {"_id": ObjectId(domain_id), "deleted": False},
        {"$set": domain_data.dict()}
    )
    if result.matched_count > 0:
        return await get_domain_by_id(domain_id)
    return None

async def patch_domain(domain_id: str, domain_data: DomainPatch) -> Optional[dict]:
    result = await db.domains.update_one(
        {"_id": ObjectId(domain_id)},
        {"$set": {**domain_data.dict(exclude_unset=True), "deleted": False}}
    )
    return await get_domain_by_id(domain_id) if result.matched_count > 0 else None

async def delete_domain(domain_id: str) -> Optional[DomainDelete]:
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

