from fastapi import APIRouter
from .indicator_routes import router as indicator_router
from .domain_routes import router as domain_router
from .data_routes import router as data_router
from .annotation_routes import router as annotation_router
from .health import router as health_router

router = APIRouter()

router.include_router(
    indicator_router, prefix="/indicators", tags=["Indicators"])
router.include_router(domain_router, prefix="/domains", tags=["Domains"])
router.include_router(data_router, prefix="/indicators", tags=["Data"])
router.include_router(annotation_router, prefix="/indicators", tags=["Annotations"])
router.include_router(health_router, prefix="/health", tags=["Health"])
