from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Optional

import db
from events import event_bus

TOOL_SCHEMAS_SPEC = [
    {
        "name": "check_doctor_availability",
        "description": (
            "Query available doctor appointment slots in the clinic database. "
            "Optionally filter by doctor_name, specialty, or date ('Today' or 'Tomorrow')."
        ),
        "properties": {
            "doctor_name": {
                "type": "string",
                "description": "Doctor name, e.g. 'Dr. Aarav Mehta' or 'Mehta'.",
            },
            "specialty": {
                "type": "string",
                "description": "Medical specialty, e.g. 'Cardiology', 'Dermatology', 'Orthopedics', 'Pediatrics'.",
            },
            "date": {
                "type": "string",
                "description": "Target date, e.g. 'Today' or 'Tomorrow'.",
            },
        },
        "required": [],
    },
    {
        "name": "book_appointment",
        "description": (
            "Book an available clinic slot for a caller AFTER the caller has explicitly "
            "confirmed their phone number read back digit-by-digit."
        ),
        "properties": {
            "caller_phone": {
                "type": "string",
                "description": "Caller's spoken phone number (digits will be normalized).",
            },
            "doctor_name": {
                "type": "string",
                "description": "Doctor name, e.g. 'Dr. Aarav Mehta'.",
            },
            "date": {
                "type": "string",
                "description": "Appointment date, e.g. 'Today' or 'Tomorrow'.",
            },
            "slot_time": {
                "type": "string",
                "description": "Appointment slot time, e.g. '10:00 AM'.",
            },
            "patient_name": {
                "type": "string",
                "description": "Optional patient name if provided by the caller.",
            },
        },
        "required": ["caller_phone", "doctor_name", "date", "slot_time"],
    },
    {
        "name": "cancel_appointment",
        "description": (
            "Cancel an existing active appointment matching caller_phone AFTER the caller "
            "has explicitly confirmed their phone number read back digit-by-digit."
        ),
        "properties": {
            "caller_phone": {
                "type": "string",
                "description": "Caller's spoken phone number used when booking.",
            },
            "appointment_id": {
                "type": "string",
                "description": "Optional booking ID (e.g. 'APT-1A2B3C') if known or disambiguating.",
            },
        },
        "required": ["caller_phone"],
    },
]


async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Run a clinic tool against SQLite and broadcast real-time dashboard events."""
    t0 = time.perf_counter()

    if tool_name == "check_doctor_availability":
        result = db.check_doctor_availability(
            doctor_name=arguments.get("doctor_name"),
            specialty=arguments.get("specialty"),
            date=arguments.get("date"),
            db_path=db_path,
        )
        doctor_highlight = arguments.get("doctor_name")
        if not doctor_highlight and result.get("slots"):
            names = {s["doctor_name"] for s in result["slots"]}
            if len(names) == 1:
                doctor_highlight = next(iter(names))
    elif tool_name == "book_appointment":
        result = db.book_appointment(
            caller_phone=str(arguments.get("caller_phone", "")),
            doctor_name=str(arguments.get("doctor_name", "")),
            date=str(arguments.get("date", "")),
            slot_time=str(arguments.get("slot_time", "")),
            patient_name=arguments.get("patient_name"),
            db_path=db_path,
        )
        doctor_highlight = result.get("doctor_name") or arguments.get("doctor_name")
    elif tool_name == "cancel_appointment":
        result = db.cancel_appointment(
            caller_phone=str(arguments.get("caller_phone", "")),
            appointment_id=arguments.get("appointment_id"),
            db_path=db_path,
        )
        doctor_highlight = result.get("doctor_name")
    else:
        result = {"ok": False, "error": f"Unknown tool: {tool_name}"}
        doctor_highlight = None

    latency_ms = round((time.perf_counter() - t0) * 1000, 1)

    await event_bus.broadcast(
        "TOOL_INVOKED",
        {
            "tool": tool_name,
            "args": arguments,
            "result": result,
            "doctor_name": doctor_highlight,
            "latency_ms": latency_ms,
        },
    )

    if tool_name == "book_appointment" and result.get("ok"):
        await event_bus.broadcast(
            "DB_UPDATED",
            {
                "action": "BOOKED",
                "appointment": result,
                "state": db.get_state(db_path),
            },
        )
        await event_bus.broadcast(
            "CONFIRMATION_CARD",
            {
                "action": "BOOKED",
                "booking_id": result["booking_id"],
                "caller_phone": result["caller_phone"],
                "patient_name": result.get("patient_name"),
                "doctor_name": result["doctor_name"],
                "specialty": result["specialty"],
                "date": result["date"],
                "slot_time": result["slot_time"],
                "status": "BOOKED",
            },
        )
    elif tool_name == "cancel_appointment" and result.get("ok"):
        await event_bus.broadcast(
            "DB_UPDATED",
            {
                "action": "CANCELLED",
                "appointment": result,
                "state": db.get_state(db_path),
            },
        )
        await event_bus.broadcast(
            "CONFIRMATION_CARD",
            {
                "action": "CANCELLED",
                "booking_id": result["booking_id"],
                "caller_phone": result["caller_phone"],
                "patient_name": result.get("patient_name"),
                "doctor_name": result["doctor_name"],
                "specialty": result["specialty"],
                "date": result["date"],
                "slot_time": result["slot_time"],
                "status": "CANCELLED",
            },
        )

    return result
