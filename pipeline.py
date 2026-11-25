from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import aiohttp
from fastapi import WebSocket
from loguru import logger
from pipecat.adapters.llm.schemas.function_schema import FunctionSchema
from pipecat.adapters.llm.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    Frame,
    InterimTranscriptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMRunFrame,
    LLMTextFrame,
    MetricsFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TranscriptionFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.metrics.metrics import ProcessingMetricsData, TTFBMetricsData
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.serializers.protobuf import ProtobufFrameSerializer
from pipecat.services.elevenlabs.stt import ElevenLabsSTTService
from pipecat.services.elevenlabs.tts import ElevenLabsTTSService
from pipecat.services.google.llm import GoogleLLMService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from events import event_bus
from tools import TOOL_SCHEMAS_SPEC, execute_tool

SYSTEM_PROMPT = (
    "You are the warm, efficient voice receptionist for Aether Clinic. "
    "Respond in 1 to 2 short, natural spoken sentences per turn. "
    "Never use markdown, asterisks, bullet points, numbered lists, or special formatting in your output. "
    "Our doctors and specialties are: Dr. Aarav Mehta (Cardiology), Dr. Priya Nair (Dermatology), "
    "Dr. Vikram Sethi (Orthopedics), and Dr. Ananya Rao (Pediatrics), with slots for Today and Tomorrow. "
    "Always use check_doctor_availability before quoting open times. "
    "CRITICAL IDENTITY & PHONE RULE: The caller is identified ONLY by the phone number they say out loud. "
    "Before calling book_appointment or cancel_appointment, you MUST explicitly read their spoken phone number "
    "back digit-by-digit grouped naturally, for example: "
    "'Just to confirm, your number is 987-654-3210, is that right?' "
    "and WAIT for the caller to clearly say Yes or confirm before executing book_appointment or cancel_appointment. "
    "After booking or cancelling, concisely state the booking ID and appointment details."
)


class TelemetryTap(FrameProcessor):
    """Pass-through frame observer that broadcasts live transcripts and stage telemetry."""

    def __init__(self, call_id: str, role: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._call_id = call_id
        self._role = role
        self._assistant_buffer: list[str] = []

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if self._role == "stt":
            if isinstance(frame, VADUserStartedSpeakingFrame):
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "VAD", "active": True},
                )
            elif isinstance(frame, VADUserStoppedSpeakingFrame):
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "VAD", "active": False},
                )
            elif isinstance(frame, InterimTranscriptionFrame) and frame.text.strip():
                await event_bus.broadcast(
                    "TRANSCRIPT_DELTA",
                    {
                        "call_id": self._call_id,
                        "speaker": "caller",
                        "text": frame.text.strip(),
                        "final": False,
                    },
                )
            elif isinstance(frame, TranscriptionFrame) and frame.text.strip():
                await event_bus.broadcast(
                    "TRANSCRIPT_DELTA",
                    {
                        "call_id": self._call_id,
                        "speaker": "caller",
                        "text": frame.text.strip(),
                        "final": True,
                    },
                )
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "STT", "active": False},
                )
        elif self._role == "llm":
            if isinstance(frame, LLMFullResponseStartFrame):
                self._assistant_buffer.clear()
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "LLM", "active": True},
                )
            elif isinstance(frame, LLMTextFrame) and frame.text:
                self._assistant_buffer.append(frame.text)
                await event_bus.broadcast(
                    "TRANSCRIPT_DELTA",
                    {
                        "call_id": self._call_id,
                        "speaker": "assistant",
                        "text": "".join(self._assistant_buffer).strip(),
                        "final": False,
                    },
                )
            elif isinstance(frame, LLMFullResponseEndFrame):
                full_text = "".join(self._assistant_buffer).strip()
                if full_text:
                    await event_bus.broadcast(
                        "TRANSCRIPT_DELTA",
                        {
                            "call_id": self._call_id,
                            "speaker": "assistant",
                            "text": full_text,
                            "final": True,
                        },
                    )
                self._assistant_buffer.clear()
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "LLM", "active": False},
                )
        elif self._role == "tts":
            if isinstance(frame, TTSStartedFrame):
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "TTS", "active": True},
                )
            elif isinstance(frame, TTSStoppedFrame):
                await event_bus.broadcast(
                    "PIPELINE_STAGE",
                    {"call_id": self._call_id, "stage": "TTS", "active": False},
                )

        if isinstance(frame, MetricsFrame):
            for data in frame.data:
                if isinstance(data, (TTFBMetricsData, ProcessingMetricsData)):
                    await event_bus.broadcast(
                        "PIPELINE_STAGE",
                        {
                            "call_id": self._call_id,
                            "stage": data.processor,
                            "metric": "ttfb" if isinstance(data, TTFBMetricsData) else "processing",
                            "latency_ms": round(data.value * 1000, 1),
                        },
                    )

        await self.push_frame(frame, direction)


def build_tools_schema() -> ToolsSchema:
    schemas = [
        FunctionSchema(
            name=spec["name"],
            description=spec["description"],
            properties=spec["properties"],
            required=spec["required"],
        )
        for spec in TOOL_SCHEMAS_SPEC
    ]
    return ToolsSchema(standard_tools=schemas)


def _register_clinic_tools(llm: GoogleLLMService) -> None:
    for spec in TOOL_SCHEMAS_SPEC:
        tool_name = spec["name"]

        async def _handler(params: FunctionCallParams, _name: str = tool_name) -> None:
            args = dict(params.arguments or {})
            result = await execute_tool(_name, args)
            await params.result_callback(result)

        llm.register_function(tool_name, _handler)


async def run_voice_pipeline(websocket: WebSocket) -> None:
    """Run an end-to-end Pipecat voice session for a browser WebSocket client."""
    elevenlabs_key = os.getenv("ELEVENLABS_API_KEY", "").strip()
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    voice_id = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM").strip() or "21m00Tcm4TlvDq8ikWAM"

    if not elevenlabs_key or not gemini_key:
        await websocket.accept()
        await event_bus.broadcast(
            "CALL_STATE",
            {
                "state": "ERROR",
                "error": "Missing ELEVENLABS_API_KEY or GEMINI_API_KEY in environment.",
            },
        )
        await websocket.close(code=4001, reason="Missing ELEVENLABS_API_KEY or GEMINI_API_KEY")
        return

    call_id = f"CALL-{uuid.uuid4().hex[:6].upper()}"
    await websocket.accept()

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_in_sample_rate=16000,
            audio_in_channels=1,
            audio_out_enabled=True,
            audio_out_sample_rate=24000,
            audio_out_channels=1,
            serializer=ProtobufFrameSerializer(),
        ),
    )

    async with aiohttp.ClientSession() as http_session:
        vad_processor = VADProcessor(vad_analyzer=SileroVADAnalyzer())

        stt = ElevenLabsSTTService(
            api_key=elevenlabs_key,
            aiohttp_session=http_session,
            sample_rate=16000,
            settings=ElevenLabsSTTService.Settings(model="scribe_v1"),
        )

        llm = GoogleLLMService(
            api_key=gemini_key,
            settings=GoogleLLMService.Settings(
                model="gemini-2.0-flash",
                system_instruction=SYSTEM_PROMPT,
            ),
        )
        _register_clinic_tools(llm)

        tts = ElevenLabsTTSService(
            api_key=elevenlabs_key,
            sample_rate=24000,
            settings=ElevenLabsTTSService.Settings(
                voice=voice_id,
                model="eleven_flash_v2_5",
            ),
        )

        context = LLMContext(
            messages=[
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT,
                },
                {
                    "role": "user",
                    "content": "Greet the caller warmly as Aether Clinic and ask how you can help with an appointment today.",
                },
            ],
            tools=build_tools_schema(),
        )
        context_aggregator = LLMContextAggregatorPair(context)

        stt_tap = TelemetryTap(call_id=call_id, role="stt")
        llm_tap = TelemetryTap(call_id=call_id, role="llm")
        tts_tap = TelemetryTap(call_id=call_id, role="tts")

        pipeline = Pipeline(
            [
                transport.input(),
                vad_processor,
                stt,
                stt_tap,
                context_aggregator.user(),
                llm,
                llm_tap,
                tts,
                tts_tap,
                transport.output(),
                context_aggregator.assistant(),
            ]
        )

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                audio_in_sample_rate=16000,
                audio_out_sample_rate=24000,
                enable_metrics=True,
                enable_usage_metrics=False,
            ),
        )

        @transport.event_handler("on_client_connected")
        async def on_client_connected(_transport: Any, _client: Any) -> None:
            logger.info(f"[{call_id}] Browser voice client connected")
            await event_bus.broadcast("CALL_STATE", {"state": "CONNECTED", "call_id": call_id})
            await task.queue_frames([LLMRunFrame()])

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(_transport: Any, _client: Any) -> None:
            logger.info(f"[{call_id}] Browser voice client disconnected")
            await task.cancel()

        runner = PipelineRunner(handle_sigint=False)
        try:
            await runner.run(task)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception(f"[{call_id}] Pipeline error: {exc}")
            await event_bus.broadcast(
                "CALL_STATE",
                {"state": "ERROR", "call_id": call_id, "error": str(exc)},
            )
        finally:
            await event_bus.broadcast("CALL_STATE", {"state": "ENDED", "call_id": call_id})
