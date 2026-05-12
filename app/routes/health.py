from fastapi import APIRouter, HTTPException
from dependencies.rabbitmq import rabbitmq_client

router = APIRouter()


@router.get("/")
async def health_check():
    mq_ok = await rabbitmq_client.health_probe()
    if mq_ok:
        return {"status": "ok", "rabbitmq": True}
    raise HTTPException(status_code=503, detail={"rabbitmq": False})
