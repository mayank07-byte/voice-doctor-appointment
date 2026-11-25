# Aether Clinic OS — Voice AI Doctor Appointment Assistant

Production-grade real-time voice receptionist built with **Pipecat**, **FastAPI**, **SQLite**, and **React + Tailwind CSS**.

## Architecture

```
Browser Mic (16kHz PCM Mono)
  │  Protobuf Frame (AudioRawFrame) over ws://localhost:8000/ws/voice
  ▼
FastAPIWebsocketTransport + SileroVADAnalyzer
  ▼
ElevenLabsSTTService (scribe_v1)
  ▼
LLMUserContextAggregator ──► GoogleLLMService (gemini-2.0-flash)
  │                                │
  │                                ├── check_doctor_availability()
  │                                ├── book_appointment()      [digits-only phone + atomic SQLite lock]
  │                                └── cancel_appointment()    [frees slot back to AVAILABLE]
  ▼                                │
ElevenLabsTTSService (eleven_flash_v2_5)
  ▼
Browser Speaker (gapless PCM playback + 24-bar AnalyserNode waveform)

Parallel Telemetry Channel: ws://localhost:8000/ws/events
  └─ Broadcasts TRANSCRIPT_DELTA, CALL_STATE, TOOL_INVOKED, DB_UPDATED, CONFIRMATION_CARD, PIPELINE_STAGE
```

## Setup

### 1. Environment Variables
Copy `.env.example` to `.env` in the project root and populate your keys:

```bash
cp .env.example .env
```

Required variables:
- `ELEVENLABS_API_KEY`
- `ELEVENLABS_VOICE_ID` (defaults to `21m00Tcm4TlvDq8ikWAM`)
- `GEMINI_API_KEY`

### 2. Backend (`backend/`)

```bash
cd backend
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS / Linux:
# source .venv/bin/activate

pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Verify backend tests:
```bash
pytest tests/test_clinic.py -v
```

### 3. Frontend (`frontend/`)

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`, click **Try Call**, and speak to book or cancel an appointment. The assistant will always read back your spoken phone number digit-by-digit for explicit confirmation before booking or cancelling.

Verify frontend unit tests & build:
```bash
npm test
npm run build
```

## REST & Debug Endpoints
- `GET /api/health` — Engine status and API key readiness
- `GET /api/state` — Doctors, live slots, and appointments ledger
- `POST /api/reset` — Reset `clinic.db` to seeded state
- `POST /api/simulate-tool` — Invoke clinic tools directly (`check_doctor_availability`, `book_appointment`, `cancel_appointment`) and trigger live `/ws/events` updates
REST & Debug Endpoints
Method	Path	Description
GET	/api/health	Engine status & API‑key readiness
GET	/api/state	Doctors, live slots, and appointments ledger
POST	/api/reset	Reset clinic.db to seeded state
POST	/api/simulate-tool	Invoke clinic tools directly (check_doctor_availability, book_appointment, cancel_appointment) and trigger live /ws/events updates
WS	/ws/events	Real‑time telemetry (transcripts, tool calls, DB updates, etc.)
WS	/ws/voice	Audio stream for the voice pipeline
Important notes
Phone number handling – The assistant reads back the caller’s number digit‑by‑digit and waits for an explicit “Yes” before any book_appointment or cancel_appointment call. This satisfies the critical identity rule in SYSTEM_PROMPT.
WebSocket URL – The frontend now points to ws://localhost:8000/ws/events (backend port 8000). If you run the backend on another port, adjust the URL in frontend/src/App.tsx accordingly.
.gitignore – .env is already ignored; never commit your secret keys.
Embedded repo – The project contains an embedded pony repository. If you don’t need it, you can remove it with git rm --cached ponytail.
