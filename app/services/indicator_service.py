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
    sort_direction = 1 if sort_order.lower() == "asc" else -1

    field_mapping = {
        "name": "name",
        "periodicity": "periodicity",
        "favourites": "favourites"
    }

    db_field = field_mapping.get(sort_by, "name")
    sort_criteria = [(db_field, sort_direction)]

    filter_criteria = {"deleted": False}
    if governance_filter is not None:
        filter_criteria["governance"] = governance_filter

    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def get_indicators_count() -> int:
    """Get total count of non-deleted indicators"""
    count = await db.indicators.count_documents({"deleted": False})
    return count


async def get_indicators_count_by_domain(domain_id: str, governance_filter: bool = None) -> int:
    """Get total count of indicators for a specific domain"""
    filter_criteria = {"domain": ObjectId(domain_id), "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance"] = governance_filter
    
    count = await db.indicators.count_documents(filter_criteria)
    return count


async def get_indicators_count_by_subdomain(domain_id: str, subdomain_name: str, governance_filter: bool = None) -> int:
    """Get total count of indicators for a specific subdomain"""
    filter_criteria = {"domain": ObjectId(domain_id), "subdomain": subdomain_name, "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance"] = governance_filter
    
    count = await db.indicators.count_documents(filter_criteria)
    return count


async def search_indicators(query: str, skip: int = 0, limit: int = 10, sort_by: str = "name", sort_order: str = "asc", governance_filter: bool = None, domain_filter: str = None, subdomain_filter: str = None) -> List[dict]:
    """Search indicators by name, description, or subdomain with word-based matching and relevance scoring"""
    if not query or len(query.strip()) < 2:
        return []
    
    # Split query into individual words
    words = [word.strip() for word in query.strip().split() if word.strip()]
    if not words:
        return []
    
    try:
        # Build search criteria for each word
        word_patterns = []
        for word in words:
            word_pattern = {"$regex": word, "$options": "i"}
            word_patterns.append({
                "$or": [
                    {"name": word_pattern},
                    {"description": word_pattern},
                    {"subdomain": word_pattern}
                ]
            })
        
        # Find indicators that match any of the words
        search_criteria = {
            "$and": [
                {"deleted": False},
                {"$or": word_patterns}
            ]
        }
        
        # Add governance filter if specified
        if governance_filter is not None:
            search_criteria["$and"].append({"governance": governance_filter})
            
        # Add domain filter if specified
        if domain_filter is not None:
            search_criteria["$and"].append({"domain": ObjectId(domain_filter)})
            
        # Add subdomain filter if specified
        if subdomain_filter is not None:
            search_criteria["$and"].append({"subdomain": subdomain_filter})
        
        indicators = await db.indicators.find(search_criteria).to_list(None)

        scored_indicators = []
        for indicator in indicators:
            score = calculate_relevance_score(indicator, words)
            if score > 0:
                scored_indicators.append((indicator, score))
        
        # Sort by relevance score or specified field
        if sort_by == "relevance" or sort_by not in ["name", "periodicity", "favourites"]:
            # Sort by relevance score (highest first)
            scored_indicators.sort(key=lambda x: x[1], reverse=True)
        else:
            # Sort by specified field
            reverse_order = (sort_order.lower() == "desc")
            scored_indicators.sort(key=lambda x: x[0].get(sort_by, ""), reverse=reverse_order)
        
        # Apply pagination after sorting
        paginated_indicators = scored_indicators[skip:skip + limit]
        
        result = []
        for indicator, score in paginated_indicators:
            serialized = serialize(indicator)
            serialized["_relevance_score"] = score

            domain_id = indicator.get("domain")
            if domain_id:
                if isinstance(domain_id, str):
                    domain_id = ObjectId(domain_id)

                domain = await db.domains.find_one({"_id": domain_id, "deleted": False})
                if domain:
                    serialized["domain"] = serialize(domain)

            result.append(serialized)

        return result
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"Database connection error while searching indicators: {e}")
        return []
    except ValueError as e:
        logger.error(f"Invalid search parameters: {e}")
        return []


def calculate_relevance_score(indicator: dict, words: List[str]) -> float:
    """Calculate relevance score based on word matches in different fields"""
    score = 0.0
    name = (indicator.get("name") or "").lower()
    description = (indicator.get("description") or "").lower()
    subdomain = (indicator.get("subdomain") or "").lower()
    
    for word in words:
        word_lower = word.lower()
        
        # Exact word match in name (highest priority)
        if word_lower in name.split():
            score += 10.0
        # Partial match in name
        elif word_lower in name:
            score += 5.0
            
        # Exact word match in subdomain
        if word_lower in subdomain.split():
            score += 3.0
        # Partial match in subdomain
        elif word_lower in subdomain:
            score += 1.5
            
        # Exact word match in description
        if word_lower in description.split():
            score += 2.0
        # Partial match in description
        elif word_lower in description:
            score += 1.0
    
    # Bonus for matching more words
    total_words = len(words)
    matched_words = sum(1 for word in words if word.lower() in name or word.lower() in description or word.lower() in subdomain)
    if matched_words > 1:
        score += (matched_words / total_words) * 5.0
    
    return score


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

    if isinstance(domain_id, str):
        domain_id = ObjectId(domain_id)

    domain = await db.domains.find_one({"_id": domain_id, "deleted": False})
    if not domain:
        logger.error(f"Domain not found for indicator {indicator_id}")
        return None

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
    sort_direction = 1 if sort_order.lower() == "asc" else -1

    field_mapping = {
        "name": "name",
        "periodicity": "periodicity",
        "favourites": "favourites"
    }

    db_field = field_mapping.get(sort_by, "name")
    sort_criteria = [(db_field, sort_direction)]

    filter_criteria = {"domain": ObjectId(domain_id), "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance"] = governance_filter

    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def get_indicators_by_subdomain(domain_id: str, subdomain_name: str, skip: int = 0, limit: int = 10, sort_by: str = "name", sort_order: str = "asc", governance_filter: bool = None) -> List[dict]:
    sort_direction = 1 if sort_order.lower() == "asc" else -1

    field_mapping = {
        "name": "name",
        "periodicity": "periodicity",
        "favourites": "favourites"
    }

    db_field = field_mapping.get(sort_by, "name")
    sort_criteria = [(db_field, sort_direction)]

    filter_criteria = {"domain": ObjectId(domain_id), "subdomain": subdomain_name, "deleted": False}
    if governance_filter is not None:
        filter_criteria["governance"] = governance_filter

    indicators = await db.indicators.find(filter_criteria).sort(sort_criteria).skip(skip).limit(limit).to_list(limit)
    return [serialize(indicator) for indicator in indicators]


async def add_resource_to_indicator(indicator_id: str, resource_id: str) -> Optional[dict]:
    """Add a resource to an indicator (idempotent - no error if already present)"""
    result = await db.indicators.update_one(
        {"_id": ObjectId(indicator_id), "deleted": False},
        {"$addToSet": {"resources": resource_id}}
    )
    if result.matched_count > 0:
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
