from __future__ import annotations

import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "clinic.db"

SEED_DOCTORS: list[tuple[str, str, list[tuple[str, list[str]]]]] = [
    (
        "Dr. Aarav Mehta",
        "Cardiology",
        [
            ("Today", ["10:00 AM", "11:30 AM", "04:00 PM"]),
            ("Tomorrow", ["09:30 AM", "02:00 PM"]),
        ],
    ),
    (
        "Dr. Priya Nair",
        "Dermatology",
        [
            ("Today", ["12:00 PM", "03:30 PM"]),
            ("Tomorrow", ["11:00 AM", "04:30 PM"]),
        ],
    ),
    (
        "Dr. Vikram Sethi",
        "Orthopedics",
        [
            ("Today", ["01:00 PM", "05:00 PM"]),
            ("Tomorrow", ["10:30 AM", "03:00 PM"]),
        ],
    ),
    (
        "Dr. Ananya Rao",
        "Pediatrics",
        [
            ("Today", ["10:30 AM", "02:30 PM"]),
            ("Tomorrow", ["09:00 AM", "01:30 PM"]),
        ],
    ),
]


def resolve_db_path(db_path: Optional[str | Path] = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    env_path = os.getenv("CLINIC_DB_PATH")
    return Path(env_path) if env_path else DEFAULT_DB_PATH


@contextmanager
def _connect(db_path: Optional[str | Path] = None) -> Iterator[sqlite3.Connection]:
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def normalize_phone(phone: str) -> str:
    """Keep ASCII digits only so spoken separators never cause lookup misses."""
    return re.sub(r"\D", "", phone or "")


def normalize_date_label(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    cleaned = date_str.strip().lower()
    if cleaned in {"today", "tod"}:
        return "Today"
    if cleaned in {"tomorrow", "tmrw", "tom"}:
        return "Tomorrow"
    local_today = datetime.now().date()
    if cleaned == local_today.isoformat():
        return "Today"
    if cleaned == (local_today + timedelta(days=1)).isoformat():
        return "Tomorrow"
    return date_str.strip()


def normalize_slot_time(slot_time: str) -> str:
    """Normalize '4:00 pm', '16:00', or '4 pm' into canonical '04:00 PM'."""
    raw = re.sub(r"\s+", " ", (slot_time or "").strip().upper())
    for fmt in ("%I:%M %p", "%I %p", "%H:%M"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%I:%M %p")
        except ValueError:
            continue
    return raw


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS doctors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            specialty TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doctor_id INTEGER NOT NULL REFERENCES doctors(id) ON DELETE CASCADE,
            date TEXT NOT NULL,
            slot_time TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'BOOKED')) DEFAULT 'AVAILABLE',
            UNIQUE (doctor_id, date, slot_time)
        );

        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id TEXT NOT NULL UNIQUE,
            caller_phone TEXT NOT NULL,
            patient_name TEXT,
            doctor_id INTEGER NOT NULL REFERENCES doctors(id) ON DELETE CASCADE,
            slot_id INTEGER NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
            date TEXT NOT NULL,
            slot_time TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('BOOKED', 'CANCELLED')) DEFAULT 'BOOKED',
            created_at TEXT NOT NULL
        );
        """
    )


def _seed_data(conn: sqlite3.Connection) -> None:
    for name, specialty, days in SEED_DOCTORS:
        cur = conn.execute(
            "INSERT INTO doctors (name, specialty) VALUES (?, ?)",
            (name, specialty),
        )
        doctor_id = cur.lastrowid
        for date_label, times in days:
            for slot_time in times:
                conn.execute(
                    "INSERT INTO slots (doctor_id, date, slot_time, status) VALUES (?, ?, ?, 'AVAILABLE')",
                    (doctor_id, date_label, slot_time),
                )


def init_db(db_path: Optional[str | Path] = None) -> None:
    with _connect(db_path) as conn:
        _create_schema(conn)
        count = conn.execute("SELECT COUNT(*) FROM doctors").fetchone()[0]
        if count == 0:
            conn.execute("BEGIN IMMEDIATE")
            try:
                if conn.execute("SELECT COUNT(*) FROM doctors").fetchone()[0] == 0:
                    _seed_data(conn)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise


def reset_db(db_path: Optional[str | Path] = None) -> dict[str, Any]:
    with _connect(db_path) as conn:
        _create_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM appointments")
            conn.execute("DELETE FROM slots")
            conn.execute("DELETE FROM doctors")
            _seed_data(conn)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return get_state(db_path)


def get_state(db_path: Optional[str | Path] = None) -> dict[str, Any]:
    with _connect(db_path) as conn:
        doctor_rows = conn.execute(
            "SELECT id, name, specialty FROM doctors ORDER BY id ASC"
        ).fetchall()
        slot_rows = conn.execute(
            """
            SELECT id, doctor_id, date, slot_time, status
            FROM slots
            ORDER BY CASE date WHEN 'Today' THEN 0 WHEN 'Tomorrow' THEN 1 ELSE 2 END, id ASC
            """
        ).fetchall()
        appt_rows = conn.execute(
            """
            SELECT
                a.id,
                a.booking_id,
                a.caller_phone,
                a.patient_name,
                a.doctor_id,
                d.name AS doctor_name,
                d.specialty AS specialty,
                a.slot_id,
                a.date,
                a.slot_time,
                a.status,
                a.created_at
            FROM appointments a
            JOIN doctors d ON d.id = a.doctor_id
            ORDER BY a.id DESC
            """
        ).fetchall()

    slots_by_doc: dict[int, list[dict[str, Any]]] = {}
    for row in slot_rows:
        slots_by_doc.setdefault(row["doctor_id"], []).append(dict(row))

    doctors = []
    for doc in doctor_rows:
        doc_dict = dict(doc)
        doc_dict["slots"] = slots_by_doc.get(doc["id"], [])
        doctors.append(doc_dict)

    return {
        "doctors": doctors,
        "appointments": [dict(r) for r in appt_rows],
    }


def _match_doctor_clause(doctor_name: Optional[str]) -> tuple[str, list[Any]]:
    if not doctor_name:
        return "", []
    cleaned = re.sub(r"^\s*dr\.?\s*", "", doctor_name.strip(), flags=re.IGNORECASE)
    return " AND LOWER(d.name) LIKE ?", [f"%{cleaned.lower()}%"]


def check_doctor_availability(
    doctor_name: Optional[str] = None,
    specialty: Optional[str] = None,
    date: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    query = """
        SELECT
            s.id AS slot_id,
            d.id AS doctor_id,
            d.name AS doctor_name,
            d.specialty AS specialty,
            s.date AS date,
            s.slot_time AS slot_time,
            s.status AS status
        FROM slots s
        JOIN doctors d ON d.id = s.doctor_id
        WHERE s.status = 'AVAILABLE'
    """
    params: list[Any] = []

    doc_clause, doc_params = _match_doctor_clause(doctor_name)
    query += doc_clause
    params.extend(doc_params)

    if specialty:
        query += " AND LOWER(d.specialty) LIKE ?"
        params.append(f"%{specialty.strip().lower()}%")

    norm_date = normalize_date_label(date)
    if norm_date:
        query += " AND LOWER(s.date) = ?"
        params.append(norm_date.lower())

    query += " ORDER BY d.id ASC, CASE s.date WHEN 'Today' THEN 0 WHEN 'Tomorrow' THEN 1 ELSE 2 END, s.id ASC"

    with _connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    slots = [dict(r) for r in rows]
    return {
        "ok": True,
        "count": len(slots),
        "slots": slots,
    }


def book_appointment(
    caller_phone: str,
    doctor_name: str,
    date: str,
    slot_time: str,
    patient_name: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    norm_phone = normalize_phone(caller_phone)
    if len(norm_phone) < 7:
        return {
            "ok": False,
            "error": "A valid caller phone number with at least 7 digits is required.",
        }

    norm_date = normalize_date_label(date)
    norm_time = normalize_slot_time(slot_time)
    if not doctor_name or not norm_date or not norm_time:
        return {
            "ok": False,
            "error": "Doctor name, date, and slot time are required to book.",
        }

    doc_clause, doc_params = _match_doctor_clause(doctor_name)

    with _connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            slot_row = conn.execute(
                f"""
                SELECT
                    s.id AS slot_id,
                    s.status AS status,
                    s.date AS date,
                    s.slot_time AS slot_time,
                    d.id AS doctor_id,
                    d.name AS doctor_name,
                    d.specialty AS specialty
                FROM slots s
                JOIN doctors d ON d.id = s.doctor_id
                WHERE LOWER(s.date) = ? AND UPPER(s.slot_time) = ? {doc_clause}
                LIMIT 1
                """,
                [norm_date.lower(), norm_time.upper(), *doc_params],
            ).fetchone()

            if not slot_row:
                conn.execute("ROLLBACK")
                return {
                    "ok": False,
                    "error": f"No matching slot found for {doctor_name} on {norm_date} at {norm_time}.",
                }

            if slot_row["status"] != "AVAILABLE":
                conn.execute("ROLLBACK")
                return {
                    "ok": False,
                    "error": f"{slot_row['doctor_name']} at {slot_row['slot_time']} on {slot_row['date']} is already booked.",
                }

            booking_id = f"APT-{uuid.uuid4().hex[:6].upper()}"
            created_at = datetime.now(timezone.utc).isoformat()
            clean_patient = patient_name.strip() if patient_name else None

            conn.execute(
                "UPDATE slots SET status = 'BOOKED' WHERE id = ?",
                (slot_row["slot_id"],),
            )
            conn.execute(
                """
                INSERT INTO appointments (
                    booking_id, caller_phone, patient_name, doctor_id, slot_id, date, slot_time, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'BOOKED', ?)
                """,
                (
                    booking_id,
                    norm_phone,
                    clean_patient,
                    slot_row["doctor_id"],
                    slot_row["slot_id"],
                    slot_row["date"],
                    slot_row["slot_time"],
                    created_at,
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "ok": True,
        "booking_id": booking_id,
        "caller_phone": norm_phone,
        "patient_name": clean_patient,
        "doctor_id": slot_row["doctor_id"],
        "doctor_name": slot_row["doctor_name"],
        "specialty": slot_row["specialty"],
        "slot_id": slot_row["slot_id"],
        "date": slot_row["date"],
        "slot_time": slot_row["slot_time"],
        "status": "BOOKED",
        "created_at": created_at,
    }


def cancel_appointment(
    caller_phone: str,
    appointment_id: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    norm_phone = normalize_phone(caller_phone)
    if len(norm_phone) < 7:
        return {
            "ok": False,
            "error": "A valid caller phone number with at least 7 digits is required to cancel.",
        }

    with _connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            query = """
                SELECT
                    a.id,
                    a.booking_id,
                    a.caller_phone,
                    a.patient_name,
                    a.doctor_id,
                    d.name AS doctor_name,
                    d.specialty AS specialty,
                    a.slot_id,
                    a.date,
                    a.slot_time,
                    a.status,
                    a.created_at
                FROM appointments a
                JOIN doctors d ON d.id = a.doctor_id
                WHERE a.caller_phone = ? AND a.status = 'BOOKED'
            """
            params: list[Any] = [norm_phone]
            if appointment_id:
                query += " AND UPPER(a.booking_id) = ?"
                params.append(appointment_id.strip().upper())
            query += " ORDER BY a.id DESC"

            matches = conn.execute(query, params).fetchall()
            if not matches:
                conn.execute("ROLLBACK")
                return {
                    "ok": False,
                    "error": "No active booked appointment found for that phone number.",
                }

            if len(matches) > 1 and not appointment_id:
                conn.execute("ROLLBACK")
                return {
                    "ok": False,
                    "error": "Multiple active appointments found for this phone number. Please specify the booking ID.",
                    "appointments": [
                        {
                            "booking_id": m["booking_id"],
                            "doctor_name": m["doctor_name"],
                            "date": m["date"],
                            "slot_time": m["slot_time"],
                        }
                        for m in matches
                    ],
                }

            target = matches[0]
            conn.execute(
                "UPDATE appointments SET status = 'CANCELLED' WHERE id = ?",
                (target["id"],),
            )
            conn.execute(
                "UPDATE slots SET status = 'AVAILABLE' WHERE id = ?",
                (target["slot_id"],),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return {
        "ok": True,
        "booking_id": target["booking_id"],
        "caller_phone": target["caller_phone"],
        "patient_name": target["patient_name"],
        "doctor_id": target["doctor_id"],
        "doctor_name": target["doctor_name"],
        "specialty": target["specialty"],
        "slot_id": target["slot_id"],
        "date": target["date"],
        "slot_time": target["slot_time"],
        "status": "CANCELLED",
        "created_at": target["created_at"],
    }
