from fastapi import APIRouter
from fastapi.responses import JSONResponse
from apps.synchronizer.lifespan import is_ready

router = APIRouter()


@router.get("/health")
async def health():
    if is_ready():
        return {"status": "ok"}
    return JSONResponse(status_code=503, content={"status": "starting"})
