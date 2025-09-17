from typing import List, Optional
from bson.objectid import ObjectId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from dependencies.database import db
from schemas.indicator import IndicatorCreate, IndicatorDelete
from utils.mongo_utils import serialize, deserialize
import logging

logger = logging.getLogger(__name__)


async def create_indicator(domain_id: str, subdomain_name: str, indicator_data: IndicatorCreate) -> Optional[dict]:
    indicator_dict = deserialize(indicator_data.dict())
    indicator_dict["_id"] = ObjectId()
    domain = await db.domains.find_one({"_id": ObjectId(domain_id)})
    if not domain:
        raise ValueError("Domain not found")
    if subdomain_name not in domain.get("subdomains", []):
        raise ValueError("Subdomain not found")
    indicator_dict["subdomain"] = subdomain_name
    indicator_dict["domain"] = ObjectId(domain_id)  # Store as ObjectId instead of string
    indicator_dict["deleted"] = False
    result = await db.indicators.insert_one(indicator_dict)
    if not result.inserted_id:
        raise ValueError("Failed to create indicator")
    indicator = await get_indicator_by_id(str(result.inserted_id))
    return indicator


async def get_all_indicators(skip: int = 0, limit: int = 10, sort_by: str = "name", sort_order: str = "asc", governance_filter: bool = None) -> List[dict]:
    # Define sort order
    sort_direction = 1 if sort_order.lower() == "asc" else -1
    
    # Map frontend field names to database field names
    field_mapping = {
        "name": "name",
        "periodicity": "periodicity", 
        "favourites": "favourites"
    }
    
    # Get the actual database field name
    db_field = field_mapping.get(sort_by, "name")
    
    # Create sort criteria
    sort_criteria = [(db_field, sort_direction)]
    
    # Build filter criteria
    filter_criteria = {"deleted": False}
    if governance_filter is not None:
        filter_criteria["governance_indicator"] = governance_filter
    
    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def get_indicators_count() -> int:
    """Get total count of non-deleted indicators"""
    count = await db.indicators.count_documents({"deleted": False})
    return count


async def get_indicator_by_id(indicator_id: str) -> Optional[dict]:
    """Get indicator by ID and populate its domain information"""
    indicator = await db.indicators.find_one({"_id": ObjectId(indicator_id), "deleted": False})
    if not indicator:
        return None

    domain_id = indicator.get("domain")
    if not domain_id:
        logger.error(
            f"Invalid domain structure for indicator {indicator_id}")
        return None

    # Convert domain_id to ObjectId if it's a string
    if isinstance(domain_id, str):
        domain_id = ObjectId(domain_id)

    # Get domain information
    domain = await db.domains.find_one({"_id": domain_id, "deleted": False})
    if not domain:
        logger.error(f"Domain not found for indicator {indicator_id}")
        return None

    # Update indicator with domain information
    indicator["domain"] = domain
    return serialize(indicator)


async def update_indicator(indicator_id: str, update_data: dict) -> int:
    if "domain" in update_data:
        domain_id = update_data["domain"]
        domain = await db.domains.find_one({"_id": ObjectId(domain_id)})
        if not domain:
            raise ValueError("Domain not found")
        if "subdomain" in update_data and update_data["subdomain"] not in domain.get("subdomains", []):
            raise ValueError("Subdomain not found")
        update_data["domain"] = domain_id

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


async def get_indicators_by_domain(domain_id: str, skip: int = 0, limit: int = 10, sort_by: str = "name", sort_order: str = "asc", governance_filter: bool = None) -> List[dict]:
    # Define sort order
    sort_direction = 1 if sort_order.lower() == "asc" else -1
    
    # Map frontend field names to database field names
    field_mapping = {
        "name": "name",
        "periodicity": "periodicity", 
        "favourites": "favourites"
    }
    
    # Get the actual database field name
    db_field = field_mapping.get(sort_by, "name")
    
    # Create sort criteria
    sort_criteria = [(db_field, sort_direction)]
    
    # Build filter criteria
    filter_criteria = {"domain": ObjectId(domain_id), "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance_indicator"] = governance_filter
    
    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def get_indicators_by_subdomain(domain_id: str, subdomain_name: str, skip: int = 0, limit: int = 10, sort_by: str = "name", sort_order: str = "asc", governance_filter: bool = None) -> List[dict]:
    # Define sort order
    sort_direction = 1 if sort_order.lower() == "asc" else -1
    
    # Map frontend field names to database field names
    field_mapping = {
        "name": "name",
        "periodicity": "periodicity", 
        "favourites": "favourites"
    }
    
    # Get the actual database field name
    db_field = field_mapping.get(sort_by, "name")
    
    # Create sort criteria
    sort_criteria = [(db_field, sort_direction)]
    
    # Build filter criteria
    filter_criteria = {"domain": ObjectId(domain_id), "subdomain": subdomain_name, "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance_indicator"] = governance_filter
    
    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def add_resource_to_indicator(indicator_id: str, resource_id: str) -> Optional[dict]:
    """Add a resource to an indicator"""
    result = await db.indicators.update_one(
        {"_id": ObjectId(indicator_id), "deleted": False},
        {"$addToSet": {"resources": resource_id}}
    )
    if result.modified_count > 0:
        return await get_indicator_by_id(indicator_id)
    return None


async def remove_resource_from_indicator(indicator_id: str, resource_id: str) -> Optional[dict]:
    """Remove a resource from an indicator"""
    result = await db.indicators.update_one(
        {"_id": ObjectId(indicator_id), "deleted": False},
        {"$pull": {"resources": resource_id}}
    )
    logger.info(f"Indicator ID: {indicator_id}")
    logger.info(f"Resource ID: {resource_id}")
    logger.info(f"Result: {result}")
    if result.modified_count > 0:
        indicator = await db.indicators.find_one(
            {"_id": ObjectId(indicator_id), "deleted": False}
        )
        return serialize(indicator)
    return None


async def get_indicator_resources(indicator_id: str) -> List[str]:
    """Get all resources data for an indicator"""
    indicator = await get_indicator_by_id(indicator_id)
    if not indicator:
        return []

    resources_data = await db.resource_data.find(
        {"resource_id": {"$in": indicator.get("resources", [])}}
    ).to_list(None)

    if not resources_data:
        return []

    return resources_data


async def get_indicator_by_resource(resource_id: str) -> Optional[dict]:
    """Find indicator that contains the resource"""
    try:
        indicator = await db.indicators.find_one(
            {"resources": resource_id, "deleted": False}
        )
        if not indicator:
            logger.warning(f"No indicator found for resource {resource_id}")
            return None

        # Get domain information like in get_indicator_by_id
        domain_id = indicator.get("domain")
        if not domain_id:
            logger.error(
                f"Invalid domain structure for indicator with resource {resource_id}")
            return None

        if isinstance(domain_id, str):
            domain_id = ObjectId(domain_id)

        domain = await db.domains.find_one({"_id": domain_id, "deleted": False})
        if not domain:
            logger.error(
                f"Domain not found for indicator with resource {resource_id}")
            return None

        indicator["domain"] = domain
        return serialize(indicator)

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(
            f"Error finding indicator for resource {resource_id}: {e}")
        return None
