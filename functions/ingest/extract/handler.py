"""Lambda `extract`: trae eventos de EONET para una ventana de fechas y
guarda el JSON crudo en S3 (raw/). No transforma nada aqui -- eso es trabajo
de `curate` (dominio ingest/curate), que se dispara cuando este objeto llega
a S3. Logica de paginacion/retries portada casi tal cual de main.py.

Dos formas de invocarse:
  - Programada (EventBridge, sin payload) -> ventana incremental por defecto
    (ultimos dias).
  - Con payload {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", ...} -> usada
    por el backfill (Step Functions) para pedir un mes especifico, o para
    pruebas manuales.
"""

import asyncio
import datetime as dt
import json
import logging
import os
import random

import boto3
import httpx

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
USER_AGENTS = [
    "eonet-extractor/1.0 (Python)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "eonet-historico-extractor/1.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
]
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
MAX_CONCURRENT_REQUESTS = 3
DELAY_BETWEEN_REQUESTS_MS = 500
MAX_CONCURRENT_RETRIES = 1
INITIAL_RETRY_DELAY_MS = 2000
MAX_PROGRESSIVE_RETRIES = 3

BUCKET_NAME = os.environ["BUCKET_NAME"]

# Cliente creado a nivel de modulo (fuera del handler): si Lambda reutiliza
# el mismo execution environment entre invocaciones ("warm start"), no lo
# recreamos cada vez.
s3 = boto3.client("s3")


def month_windows(start: dt.date, end: dt.date):
    windows = []
    cur = start
    while cur < end:
        year, month = cur.year, cur.month
        nxt = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
        windows.append((cur, min(nxt, end)))
        cur = nxt
    return windows


async def fetch_window(client, start, end, status, limit, category, bbox):
    params = {"status": status, "start": start.isoformat(), "end": end.isoformat(), "limit": limit}
    if category:
        params["category"] = category
    if bbox:
        params["bbox"] = bbox

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            user_agent = random.choice(USER_AGENTS)
            response = await client.get(
                BASE_URL, params=params, headers={"User-Agent": user_agent}, timeout=30
            )
            response.raise_for_status()
            logger.info("OK %s", start.isoformat())
            return response.json().get("events", []), True, start, end
        except httpx.HTTPError as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

    logger.error("FAIL %s: %s", start.isoformat(), last_error)
    return [], False, start, end


async def fetch_all(client, start, end, status, limit, category, bbox):
    windows = month_windows(start, end)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_with_delay(window_start, window_end):
        async with semaphore:
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS_MS / 1000.0)
            return await fetch_window(client, window_start, window_end, status, limit, category, bbox)

    results = await asyncio.gather(*(fetch_with_delay(ws, we) for ws, we in windows))

    events_by_id: dict[str, dict] = {}
    failed_windows = []
    for events, success, window_start, window_end in results:
        if not success:
            failed_windows.append((window_start, window_end))
        if events and len(events) >= limit:
            logger.warning("Ventana topo limite (%s); pueden faltar eventos.", limit)
        for event in events:
            events_by_id[event["id"]] = event

    return list(events_by_id.values()), failed_windows


async def retry_failed_windows(client, failed_windows, status, limit, category, bbox, retry_attempt, delay_ms):
    if not failed_windows:
        return [], []

    logger.info("Reintento #%s: %s ventana(s), delay=%sms", retry_attempt, len(failed_windows), delay_ms)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_RETRIES)

    async def fetch_with_longer_delay(window_start, window_end):
        async with semaphore:
            await asyncio.sleep(delay_ms / 1000.0)
            return await fetch_window(client, window_start, window_end, status, limit, category, bbox)

    results = await asyncio.gather(*(fetch_with_longer_delay(ws, we) for ws, we in failed_windows))

    events_by_id: dict[str, dict] = {}
    still_failed = []
    for events, success, window_start, window_end in results:
        if not success:
            still_failed.append((window_start, window_end))
        for event in events:
            events_by_id[event["id"]] = event

    return list(events_by_id.values()), still_failed


async def fetch_range(start: dt.date, end: dt.date, status="all", limit=10000, category=None, bbox=None):
    async with httpx.AsyncClient() as client:
        events, failed_windows = await fetch_all(client, start, end, status, limit, category, bbox)

        retry_count = 0
        while failed_windows and retry_count < MAX_PROGRESSIVE_RETRIES:
            retry_count += 1
            delay_ms = INITIAL_RETRY_DELAY_MS + (retry_count - 1) * 2000
            retry_events, failed_windows = await retry_failed_windows(
                client, failed_windows, status, limit, category, bbox, retry_count, delay_ms
            )
            events.extend(retry_events)
            events = list({e["id"]: e for e in events}.values())

    return events, failed_windows


def _default_window():
    """Ventana incremental por defecto: ultimos dias (con 1 dia extra de
    solapamiento por si EONET actualiza eventos recientes con retraso)."""
    # UTC explicito (no dt.date.today(), que toma la hora local del sistema)
    # -- Lambda ya corre en UTC, pero esto lo hace explicito y consistente
    # con `fetched_at` mas abajo, que ya usaba dt.timezone.utc.
    end = dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)  # `end` es exclusivo para EONET
    start = end - dt.timedelta(days=3)
    return start, end


def handler(event, context):
    event = event or {}
    start = dt.date.fromisoformat(event["start"]) if event.get("start") else None
    end = dt.date.fromisoformat(event["end"]) if event.get("end") else None
    if start is None or end is None:
        start, end = _default_window()

    status = event.get("status", "all")
    category = event.get("category")
    bbox = event.get("bbox")

    events, failed_windows = asyncio.run(fetch_range(start, end, status, category=category, bbox=bbox))

    if failed_windows:
        # Estas ventanas se quedaron sin datos pese a los reintentos. Fallamos
        # la invocacion completa para que el mecanismo de reintento de Lambda
        # (o de Step Functions, en el backfill) lo intente de nuevo despues.
        raise RuntimeError(f"{len(failed_windows)} ventana(s) sin datos tras reintentos: {failed_windows}")

    body = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "status": status,
        "category": category,
        "bbox": bbox,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event_count": len(events),
        "events": events,
    }
    # Key se usa con dt= para que Athena pueda particionar por fecha de ingesta, y con start/end
    key = f"raw/eonet/dt={end.isoformat()}/events_{start.isoformat()}_{end.isoformat()}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Escritos %s eventos en s3://%s/%s", len(events), BUCKET_NAME, key)
    return {"bucket": BUCKET_NAME, "key": key, "event_count": len(events)}
