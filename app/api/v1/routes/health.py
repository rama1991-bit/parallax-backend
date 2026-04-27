from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def health():
    return {"status": "ok"}


@router.get("/db")
async def db_health():
    return {"status": "stub", "message": "DB check not wired in scaffold."}


@router.get("/redis")
async def redis_health():
    return {"status": "stub", "message": "Redis check not wired in scaffold."}
