from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path

_SINGLE_SERVER_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_SINGLE_SERVER_DIR / ".env")
except ImportError:
    pass

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

app = FastAPI(title="Single Server Market System")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Your React URL
    allow_credentials=True,
    allow_methods=["*"],  # Allows OPTIONS, POST, GET, etc.
    allow_headers=["*"],  # Allows Content-Type and other headers
)
app.include_router(api_router)