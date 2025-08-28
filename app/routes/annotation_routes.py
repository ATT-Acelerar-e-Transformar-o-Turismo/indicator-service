from fastapi import APIRouter, HTTPException
from typing import List
from schemas.annotation import Annotation, AnnotationCreate, AnnotationUpdate
from schemas.common import PyObjectId
from bson.errors import InvalidId
from services.annotation_service import (
    create_annotation,
    get_annotations_by_indicator,
    get_annotation_by_id,
    update_annotation,
    delete_annotation
)

router = APIRouter()

ANNOTATION_NOT_FOUND = "Annotation not found"
INVALID_ANNOTATION_ID = "Invalid annotation ID"
INVALID_INDICATOR_ID = "Invalid indicator ID"


@router.get("/{indicator_id}/annotations", response_model=List[Annotation])
async def get_indicator_annotations(indicator_id: str):
    """Get all annotations for an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    annotations = await get_annotations_by_indicator(indicator_id)
    return annotations


@router.post("/{indicator_id}/annotations", response_model=Annotation)
async def create_indicator_annotation(
    indicator_id: str,
    annotation: AnnotationCreate
):
    """Create a new annotation for an indicator"""
    try:
        PyObjectId(indicator_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail=INVALID_INDICATOR_ID)

    created_annotation = await create_annotation(indicator_id, annotation)
    return created_annotation


@router.get("/{indicator_id}/annotations/{annotation_id}", response_model=Annotation)
async def get_annotation(
    indicator_id: str,
    annotation_id: str
):
    """Get a specific annotation"""
    try:
        PyObjectId(indicator_id)
        PyObjectId(annotation_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail="Invalid ID")

    annotation = await get_annotation_by_id(annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)
    
    # Verify annotation belongs to indicator
    if str(annotation.indicator_id) != indicator_id:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)

    return annotation


@router.patch("/{indicator_id}/annotations/{annotation_id}", response_model=Annotation)
async def update_indicator_annotation(
    indicator_id: str,
    annotation_id: str,
    annotation: AnnotationUpdate
):
    """Update an existing annotation"""
    try:
        PyObjectId(indicator_id)
        PyObjectId(annotation_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail="Invalid ID")

    # Verify annotation exists and belongs to indicator
    existing = await get_annotation_by_id(annotation_id)
    if not existing or str(existing.indicator_id) != indicator_id:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)

    updated_annotation = await update_annotation(annotation_id, annotation)
    if not updated_annotation:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)

    return updated_annotation


@router.delete("/{indicator_id}/annotations/{annotation_id}")
async def delete_indicator_annotation(
    indicator_id: str,
    annotation_id: str
):
    """Delete an annotation"""
    try:
        PyObjectId(indicator_id)
        PyObjectId(annotation_id)
    except (InvalidId, ValueError):
        raise HTTPException(status_code=400, detail="Invalid ID")

    # Verify annotation exists and belongs to indicator
    existing = await get_annotation_by_id(annotation_id)
    if not existing or str(existing.indicator_id) != indicator_id:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)

    success = await delete_annotation(annotation_id)
    if not success:
        raise HTTPException(status_code=404, detail=ANNOTATION_NOT_FOUND)

    return {"message": "Annotation deleted successfully"}