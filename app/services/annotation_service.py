from typing import List, Optional
from bson.objectid import ObjectId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from dependencies.database import db
from schemas.annotation import Annotation, AnnotationCreate, AnnotationUpdate
from utils.mongo_utils import serialize, deserialize
import logging

logger = logging.getLogger(__name__)


async def create_annotation(indicator_id: str, annotation: AnnotationCreate) -> Annotation:
    """Create a new annotation for an indicator"""
    try:
        annotation_data = annotation.model_dump()
        annotation_data["indicator_id"] = ObjectId(indicator_id)
        
        result = await db.annotations.insert_one(annotation_data)
        
        # Fetch the created annotation
        created_annotation = await db.annotations.find_one({"_id": result.inserted_id})
        return Annotation(**serialize(created_annotation))
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error creating annotation: {e}")
        raise


async def get_annotations_by_indicator(indicator_id: str) -> List[Annotation]:
    """Get all annotations for an indicator"""
    try:
        annotations = await db.annotations.find(
            {"indicator_id": ObjectId(indicator_id)}
        ).to_list(None)
        
        return [Annotation(**serialize(annotation)) for annotation in annotations]
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error getting annotations: {e}")
        raise


async def get_annotation_by_id(annotation_id: str) -> Optional[Annotation]:
    """Get a specific annotation by ID"""
    try:
        annotation = await db.annotations.find_one({"_id": ObjectId(annotation_id)})
        
        if annotation:
            return Annotation(**serialize(annotation))
        return None
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error getting annotation: {e}")
        raise


async def update_annotation(annotation_id: str, annotation: AnnotationUpdate) -> Optional[Annotation]:
    """Update an existing annotation"""
    try:
        update_data = {k: v for k, v in annotation.model_dump().items() if v is not None}
        
        if not update_data:
            # No fields to update
            return await get_annotation_by_id(annotation_id)
        
        result = await db.annotations.update_one(
            {"_id": ObjectId(annotation_id)},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            return await get_annotation_by_id(annotation_id)
        return None
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error updating annotation: {e}")
        raise


async def delete_annotation(annotation_id: str) -> bool:
    """Delete an annotation"""
    try:
        result = await db.annotations.delete_one({"_id": ObjectId(annotation_id)})
        return result.deleted_count > 0
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error deleting annotation: {e}")
        raise