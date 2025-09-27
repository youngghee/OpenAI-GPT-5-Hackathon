"""FastAPI-powered frontend for the self-enriching BI workflow."""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Literal
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.core.config import Settings, load_settings
from src.core.dependencies import RunnerDependencies, build_dependencies
from src.core.record_utils import build_record_context, extract_candidate_urls
from src.core.runner import run_scenario
from src.core.chat import describe_query_event, describe_scraper_event

REPO_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = REPO_ROOT / "assets" / "frontend"
DEFAULT_PRIMARY_KEY = "BRIZO_ID"
DEFAULT_TABLE_NAME = "dataset"


@dataclass(slots=True)
class SessionState:
    record_id: str
    table_name: str
    primary_key: str
    counter: int = 0


class ResultStore:
    """Thread-safe store for completed ticket results."""

    def __init__(self) -> None:
        self._results: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def set(self, ticket_id: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._results[ticket_id] = payload

    def get(self, ticket_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._results.get(ticket_id)


class RealtimeBroker:
    """Dispatches timeline events to subscribers in near real-time."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribers: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self._history: dict[str, list[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()

    async def subscribe(self, ticket_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        async with self._lock:
            subscribers = self._subscribers.setdefault(ticket_id, set())
            subscribers.add(queue)
            history = list(self._history.get(ticket_id, []))
        for event in history:
            await queue.put(event)
        return queue

    async def unsubscribe(self, ticket_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(ticket_id)
            if not subscribers:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(ticket_id, None)

    def publish(self, ticket_id: str, event: dict[str, Any]) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._publish(ticket_id, event), self._loop)

    async def _publish(self, ticket_id: str, event: dict[str, Any]) -> None:
        async with self._lock:
            subscribers = list(self._subscribers.get(ticket_id, set()))
            history = self._history.setdefault(ticket_id, [])
            history.append(event)
        for queue in subscribers:
            await queue.put(event)

    def publish_timeline(self, ticket_id: str, entry: TimelineEntry) -> None:
        self.publish(
            ticket_id,
            {
                "type": "timeline",
                "event": entry.model_dump(),
            },
        )

    def publish_result(self, ticket_id: str, result: dict[str, Any], timeline: list[TimelineEntry]) -> None:
        self.publish(
            ticket_id,
            {
                "type": "result",
                "result": result,
                "timeline": [entry.model_dump() for entry in timeline],
            },
        )


class SessionManager:
    """Thread-safe in-memory session registry for chat conversations."""

    def __init__(self) -> None:
        self._sessions: dict[str, SessionState] = {}
        self._lock = threading.Lock()

    def create(self, *, record_id: str, table_name: str, primary_key: str) -> str:
        session_id = uuid4().hex[:8]
        with self._lock:
            while session_id in self._sessions:
                session_id = uuid4().hex[:8]
            self._sessions[session_id] = SessionState(
                record_id=record_id,
                table_name=table_name,
                primary_key=primary_key,
            )
        return session_id

    def get(self, session_id: str) -> SessionState | None:
        with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                return None
            return SessionState(
                record_id=state.record_id,
                table_name=state.table_name,
                primary_key=state.primary_key,
                counter=state.counter,
            )

    def reserve_ticket(self, session_id: str) -> str:
        with self._lock:
            state = self._sessions.get(session_id)
            if state is None:
                raise KeyError(session_id)
            state.counter += 1
            return f"{session_id}-Q{state.counter:03d}"


class DatasetService:
    """Caches CSV-backed dataset access for the web frontend."""

    def __init__(self, settings: Settings) -> None:
        if settings.csv_source is None:
            raise ValueError("CSV data source is required for the frontend")
        self._settings = settings
        self.table_name = settings.csv_source.table_name or DEFAULT_TABLE_NAME
        self.default_primary_key = DEFAULT_PRIMARY_KEY
        self._executor = None

    @property
    def executor(self):
        from src.integrations.csv_sql_executor import CsvSQLExecutor

        if self._executor is None:
            path = self._settings.csv_source.resolve_path()
            self._executor = CsvSQLExecutor(csv_path=path, table_name=self.table_name)
        return self._executor

    def fetch_record(
        self,
        *,
        record_id: str,
        primary_key: str | None = None,
        table_name: str | None = None,
    ) -> dict[str, Any] | None:
        pk = primary_key or self.default_primary_key
        table = table_name or self.table_name
        safe_id = record_id.replace("'", "''")
        statement = f"SELECT * FROM {table} WHERE {pk} = '{safe_id}' LIMIT 1"
        rows = self.executor.run(statement)
        return rows[0] if rows else None


class TimelineEntry(BaseModel):
    source: str = Field(..., description="Originating component, e.g. query or scraper")
    message: str = Field(..., description="Human-readable narration of the step")


class SessionStartRequest(BaseModel):
    record_id: str = Field(..., description="Primary key for the CRM row")
    table_name: str | None = Field(None, description="Override table name for the lookup")
    primary_key_column: str | None = Field(
        None, description="Override primary key column name used for the lookup"
    )


class SessionStartResponse(BaseModel):
    session_id: str
    record_id: str
    table_name: str
    primary_key_column: str
    record: dict[str, Any]
    record_context: dict[str, Any]
    candidate_urls: list[str]


class AskQuestionRequest(BaseModel):
    question: str = Field(..., min_length=1)


class AskQuestionAcceptedResponse(BaseModel):
    session_id: str
    ticket_id: str
    record_id: str
    status: Literal["processing"] = Field("processing")


class TicketResultResponse(BaseModel):
    ticket_id: str
    result: dict[str, Any]
    timeline: list[TimelineEntry]


@dataclass(slots=True)
class TimelineRecorder:
    """Collects and publishes timeline narration to the frontend."""

    events: list[TimelineEntry]
    broker: RealtimeBroker | None = None
    ticket_id: str | None = None

    def add(self, source: str, message: str) -> None:
        if not message:
            return
        entry = TimelineEntry(source=source, message=message)
        self.events.append(entry)
        if self.broker is not None and self.ticket_id is not None:
            self.broker.publish_timeline(self.ticket_id, entry)


@dataclass(slots=True)
class FrontendQueryLogger:
    downstream: Any
    timeline: TimelineRecorder

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        if self.downstream is not None:
            self.downstream.log_event(ticket_id, event, payload)
        message = describe_query_event(event, payload)
        if message:
            self.timeline.add("query", message)


@dataclass(slots=True)
class FrontendScraperLogger:
    downstream: Any
    timeline: TimelineRecorder

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        if self.downstream is not None:
            self.downstream.log_event(ticket_id, event, payload)
        message = describe_scraper_event(event, payload)
        if message:
            self.timeline.add("scraper", message)


@dataclass(slots=True)
class TimelineUpdateAgent:
    inner: Any
    timeline: TimelineRecorder

    def __getattr__(self, item: str) -> Any:
        return getattr(self.inner, item)

    def apply_enrichment(
        self,
        *,
        ticket_id: str,
        record_id: str,
        enriched_fields: dict[str, Any],
    ) -> dict[str, Any]:
        if enriched_fields:
            self.timeline.add("update", "Passing new fields to the update agent.")
        else:
            self.timeline.add("update", "Update agent check with no new fields provided.")
        result = self.inner.apply_enrichment(
            ticket_id=ticket_id, record_id=record_id, enriched_fields=enriched_fields
        )
        status = result.get("status")
        if status:
            self.timeline.add("update", f"Update agent finished with status '{status}'.")
        return result


@dataclass(slots=True)
class TimelineSchemaAgent:
    inner: Any
    timeline: TimelineRecorder

    def __getattr__(self, item: str) -> Any:
        return getattr(self.inner, item)

    def propose_change(
        self,
        *,
        ticket_id: str,
        evidence_summary: dict[str, Any],
    ) -> dict[str, Any]:
        self.timeline.add("schema", "Escalating to schema agent for review.")
        result = self.inner.propose_change(ticket_id=ticket_id, evidence_summary=evidence_summary)
        columns = result.get("columns") or []
        if columns:
            names = [str(column.get("name")) for column in columns if isinstance(column, dict)]
            self.timeline.add("schema", "Schema agent proposed columns: " + ", ".join(names))
        else:
            self.timeline.add("schema", "Schema agent found no structural changes needed.")
        return result


def create_app(config_path: str = "configs/dev.yaml") -> FastAPI:
    settings = load_settings(config_path)
    dataset_service = DatasetService(settings)
    session_manager = SessionManager()

    app = FastAPI(title="Self-Enriching BI Frontend", version="0.1.0")
    broker = RealtimeBroker()
    result_store = ResultStore()
    app.state.settings = settings
    app.state.dataset_service = dataset_service
    app.state.session_manager = session_manager
    app.state.broker = broker
    app.state.result_store = result_store
    app.add_event_handler("startup", broker.startup)

    if FRONTEND_DIR.exists():
        app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR), html=False), name="assets")

    @app.get("/", response_model=None)
    def index():  # type: ignore[override]
        if FRONTEND_DIR.exists():
            index_path = FRONTEND_DIR / "index.html"
            if index_path.exists():
                return FileResponse(index_path)
        return JSONResponse({"message": "Frontend assets not found"}, status_code=404)

    @app.get("/api/health")
    def healthcheck() -> dict[str, str]:  # pragma: no cover - trivial
        return {"status": "ok"}

    @app.post("/api/session", response_model=SessionStartResponse)
    def start_session(payload: SessionStartRequest) -> SessionStartResponse:
        primary_key = payload.primary_key_column or dataset_service.default_primary_key
        table_name = payload.table_name or dataset_service.table_name
        record = dataset_service.fetch_record(
            record_id=payload.record_id,
            primary_key=primary_key,
            table_name=table_name,
        )
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")

        session_id = session_manager.create(
            record_id=payload.record_id,
            table_name=table_name,
            primary_key=primary_key,
        )
        context = build_record_context(record)
        candidate_urls = extract_candidate_urls(record)
        return SessionStartResponse(
            session_id=session_id,
            record_id=payload.record_id,
            table_name=table_name,
            primary_key_column=primary_key,
            record=record,
            record_context=context,
            candidate_urls=candidate_urls,
        )

    @app.get("/api/session/{session_id}", response_model=SessionStartResponse)
    def resume_session(session_id: str) -> SessionStartResponse:
        state = session_manager.get(session_id)
        if state is None:
            raise HTTPException(status_code=404, detail="Session not found")
        record = dataset_service.fetch_record(
            record_id=state.record_id,
            primary_key=state.primary_key,
            table_name=state.table_name,
        )
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")
        context = build_record_context(record)
        candidate_urls = extract_candidate_urls(record)
        return SessionStartResponse(
            session_id=session_id,
            record_id=state.record_id,
            table_name=state.table_name,
            primary_key_column=state.primary_key,
            record=record,
            record_context=context,
            candidate_urls=candidate_urls,
        )

    @app.post(
        "/api/session/{session_id}/ask",
        response_model=AskQuestionAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def ask_question(
        session_id: str,
        payload: AskQuestionRequest,
        background_tasks: BackgroundTasks,
    ) -> AskQuestionAcceptedResponse:
        state = session_manager.get(session_id)
        if state is None:
            raise HTTPException(status_code=404, detail="Session not found")

        ticket_id = session_manager.reserve_ticket(session_id)
        scenario = {
            "ticket_id": ticket_id,
            "question": payload.question,
            "record_id": state.record_id,
            "primary_key_column": state.primary_key,
            "table_name": state.table_name,
        }
        background_tasks.add_task(
            _process_question,
            settings,
            scenario,
            ticket_id,
            broker,
            result_store,
        )

        return AskQuestionAcceptedResponse(
            session_id=session_id,
            ticket_id=ticket_id,
            record_id=state.record_id,
        )

    @app.get("/api/tickets/{ticket_id}", response_model=TicketResultResponse)
    def get_ticket(ticket_id: str) -> TicketResultResponse:
        payload = result_store.get(ticket_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Result not ready")
        timeline_entries = [TimelineEntry(**entry) for entry in payload["timeline"]]
        return TicketResultResponse(
            ticket_id=ticket_id,
            result=payload["result"],
            timeline=timeline_entries,
        )

    @app.get("/api/tickets/{ticket_id}/events")
    async def stream_ticket(ticket_id: str) -> StreamingResponse:
        async def event_generator() -> AsyncIterator[str]:
            queue = await broker.subscribe(ticket_id)
            try:
                while True:
                    event = await queue.get()
                    yield _format_sse(event.get("type", "message"), json.dumps(event))
                    if event.get("type") == "result":
                        break
            finally:
                await broker.unsubscribe(ticket_id, queue)

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    return app


def _process_question(
    settings: Settings,
    scenario: dict[str, Any],
    ticket_id: str,
    broker: RealtimeBroker,
    result_store: ResultStore,
) -> None:
    dependencies = build_dependencies(settings)
    timeline = TimelineRecorder(events=[], broker=broker, ticket_id=ticket_id)
    attach_timeline(dependencies, timeline)
    try:
        result = run_scenario(dependencies, scenario)
    except Exception as exc:  # pragma: no cover - defensive
        timeline.add("system", "Question processing failed; see logs for details.")
        result = {
            "ticket_id": ticket_id,
            "status": "error",
            "error": str(exc),
        }

    payload = {
        "result": result,
        "timeline": [entry.model_dump() for entry in timeline.events],
    }
    result_store.set(ticket_id, payload)
    broker.publish_result(ticket_id, result, timeline.events)


def _format_sse(event_type: str, data: str) -> str:
    return f"event: {event_type}\ndata: {data}\n\n"


def attach_timeline(dependencies: RunnerDependencies, timeline: TimelineRecorder) -> None:
    query_downstream = getattr(dependencies, "query_logger", None)
    query_logger = FrontendQueryLogger(downstream=query_downstream, timeline=timeline)
    dependencies.query_logger = query_logger

    scraper_downstream = getattr(dependencies, "scraper_logger", None)
    scraper_logger = FrontendScraperLogger(downstream=scraper_downstream, timeline=timeline)
    dependencies.scraper_logger = scraper_logger

    if dependencies.scraper_agent is not None:
        dependencies.scraper_agent.logger = scraper_logger

    if dependencies.update_agent is not None:
        dependencies.update_agent = TimelineUpdateAgent(
            inner=dependencies.update_agent,
            timeline=timeline,
        )

    if dependencies.schema_agent is not None:
        dependencies.schema_agent = TimelineSchemaAgent(
            inner=dependencies.schema_agent,
            timeline=timeline,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the web frontend")
    parser.add_argument("--config", default="configs/dev.yaml", help="Path to configuration file")
    parser.add_argument("--host", default="127.0.0.1", help="Interface to bind the server")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind the server")
    args = parser.parse_args()

    app = create_app(config_path=args.config)

    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - defensive
        raise SystemExit("uvicorn must be installed to run the web frontend") from exc

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
