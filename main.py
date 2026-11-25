from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, APIRouter

from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import db
from events import event_bus
from pipeline import run_voice_pipeline
from tools import execute_tool

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
load_dotenv()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="Aether Clinic Voice AI Backend", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        os.getenv("FRONTEND_ORIGIN", "http://localhost:5173"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SimulateToolRequest(BaseModel):
    tool: str = Field(..., description="Tool name: check_doctor_availability, book_appointment, or cancel_appointment")
    arguments: dict[str, Any] = Field(default_factory=dict)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "engine": "pipecat",
        "vad": "silero",
        "stt": "scribe_v1",
        "llm": "gemini-2.0-flash",
        "tts": "eleven_flash_v2_5",
        "keys_configured": {
            "elevenlabs": bool(os.getenv("ELEVENLABS_API_KEY", "").strip()),
            "gemini": bool(os.getenv("GEMINI_API_KEY", "").strip()),
        },
    }


@app.get("/api/state")
def get_clinic_state() -> dict[str, Any]:
    return db.get_state()


@app.post("/api/reset")
async def reset_clinic_data() -> dict[str, Any]:
    state = db.reset_db()
    await event_bus.broadcast("DB_UPDATED", {"action": "RESET", "state": state})
    return state


@app.post("/api/simulate-tool")
async def simulate_tool(payload: SimulateToolRequest) -> dict[str, Any]:
    valid_tools = {"check_doctor_availability", "book_appointment", "cancel_appointment"}
    if payload.tool not in valid_tools:
        raise HTTPException(status_code=400, detail=f"Unsupported tool: {payload.tool}")
    result = await execute_tool(payload.tool, payload.arguments)
    return {"tool": payload.tool, "result": result, "state": db.get_state()}


@app.websocket("/ws/events")
async def events_websocket(websocket: WebSocket) -> None:
    await event_bus.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await event_bus.disconnect(websocket)
    except Exception:
        await event_bus.disconnect(websocket)


@app.websocket("/ws/voice")
async def voice_websocket(websocket: WebSocket) -> None:
    await run_voice_pipeline(websocket)
