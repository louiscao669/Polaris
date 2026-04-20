from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

_BASE = Path(__file__).resolve().parent
_BACKEND = _BASE / "Backend Functions"
for _name in (
    "Market Functions",
    "Event Functions",
    "Organization Functions",
    "User Functions",
):
    _p = str(_BACKEND / _name)
    if _p not in sys.path:
        sys.path.insert(0, _p)

from api_routes import router as api_router
from database import create_db_and_tables

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(title="Single Server Market System", lifespan=lifespan)
app.include_router(api_router)
