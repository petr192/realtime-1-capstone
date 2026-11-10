import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field

import clickhouse_connect
import httpx
import time
from clickhouse_connect.driver.exceptions import ClickHouseError
from fastapi import FastAPI, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, Gauge
from cache import TTLCache
import logging

logger = logging.getLogger(__name__)


CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "capstone")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cocojumbo")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "putmeup")

HTTP_REQUESTS = Counter("capstone_http_requests_total", "HTTP requests", ["route", "status"])
HTTP_DURATION = Histogram("capstone_http_duration_seconds", "HTTP duration", ["route"], buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),)
CACHE_REQUESTS = Counter("capstone_cache_requests_total", "Summary cache lookups", ["result"])
CLICKHOUSE_PARTS = Gauge("capstone_clickhouse_parts_total", "Number of parts in ClickHouse table", ["table"])


ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
DatasetId = Annotated[str, Path(pattern=ID_PATTERN)]
OrderId = Annotated[str, Path(pattern=ID_PATTERN)]


class NoParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

class OrderFilters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["created", "paid", "cancelled"] | None = None
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None
    limit: Annotated[int, Field(ge=1, le=100)] = 50
    after_order_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None

class TimeseriesQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_: datetime = Field(alias="from")
    to: datetime
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None

    @model_validator(mode="after")
    def check_range(self):
        for v in (self.from_, self.to):
            if v.tzinfo is None or v.utcoffset() is None:
                raise ValueError("timezone required")
            u = v.astimezone(timezone.utc)
            if u.minute or u.second or u.microsecond:
                raise ValueError("must be on the hour")
        f, t = self.from_.astimezone(timezone.utc), self.to.astimezone(timezone.utc)
        if f >= t:
            raise ValueError("from must be < to")
        if t - f > timedelta(days=31):
            raise ValueError("range too large")
        return self
    

def utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    dt = utc(dt)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

def order_to_dict(row) -> dict:
    order_id, campaign_id, version, status, amount, created_at, ingested_at = row
    return {
        "order_id": order_id,
        "campaign_id": campaign_id,
        "version": int(version),
        "status": status,
        "amount_cents": int(amount),
        "created_at": iso(created_at),
        "ingested_at": iso(ingested_at),
    }
    

async def _refresh_clickhouse_parts(ch_client):
    """Periodically query system.parts and update the gauge."""
    while True:
        try:
            res = await ch_client.query(
                "SELECT table, count() FROM system.parts WHERE active = 1 AND database = {db:String} GROUP BY table",
                parameters={"db": CLICKHOUSE_DATABASE},
                settings={"max_execution_time": 5},
            )
            for table, count in res.result_rows:
                CLICKHOUSE_PARTS.labels(table=table).set(int(count))
        except Exception:
            logger.exception("clickhouse_parts refresh failed")
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.gate = asyncio.Semaphore(4)
    app.state.cache = TTLCache(ttl=30, max_entries=128)
    app.state.http_ch_client = httpx.AsyncClient(
        base_url=CLICKHOUSE_URL,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=5,
        trust_env=False,
    )
    ch_client = await clickhouse_connect.get_async_client(
        dsn=CLICKHOUSE_URL,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=5,
        send_receive_timeout=5,
    )
    app.state.ch_client = ch_client
    asyncio.create_task(_refresh_clickhouse_parts(ch_client))
    try:
        yield
    finally:
        await ch_client.close()
        await app.state.http_ch_client.aclose()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def metrics_mw(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)
    start = time.perf_counter()
    status = "500"
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    finally:
        route = request.scope.get("route")
        path = route.path if route is not None else "unmatched"
        HTTP_REQUESTS.labels(route=path, status=status).inc()
        HTTP_DURATION.labels(route=path).observe(time.perf_counter() - start)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def ch_query(request: Request, sql: str, params: dict):
    gate = request.app.state.gate
    try:
        await asyncio.wait_for(gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
    try:
        async with asyncio.timeout(5):
            return await request.app.state.ch_client.query(
                sql, parameters=params, settings={"max_execution_time": 5}
            )
    except (ClickHouseError, TimeoutError, OSError) as error:
        logger.exception("ClickHouse query failed")
        raise HTTPException(503, "ClickHouse unavailable") from None
    finally:
        gate.release()


async def latest_event_at(request: Request, dataset_id: str) -> str | None:
    res = await ch_query(
        request,
        "SELECT maxOrNull(ingested_at) FROM capstone.orders_current WHERE dataset_id = {ds:String}",
        {"ds": dataset_id},
    )
    return iso(res.result_rows[0][0])


def meta(latest: str | None) -> dict:
    return {"computed_at": iso(datetime.now(timezone.utc)), "latest_event_at": latest}


@app.get("/ping")
async def ping():
    return {"status": "ok"}


class SummaryQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    refresh: bool = False


def _cache_key(dataset_id: str, prefix: str) -> str:
    return f"{prefix}:{dataset_id}"


@app.get("/v1/datasets/{dataset_id}/metrics/summary")
async def summary(request: Request, dataset_id: DatasetId, q: Annotated[SummaryQuery, Query()]):
    cache = request.app.state.cache
    key = _cache_key(dataset_id, "summary")
    if not q.refresh:
        cached = cache.get(key)
        if cached is not None:
            CACHE_REQUESTS.labels(result="hit").inc()
            return cached
    CACHE_REQUESTS.labels(result="miss").inc()

    res = await ch_query(
        request,
        "SELECT paid_amount_cents FROM capstone.metrics_paid_amount WHERE dataset_id = {ds:String}",
        {"ds": dataset_id},
    )
    paid = int(res.result_rows[0][0]) if res.result_rows else 0
    latest = await latest_event_at(request, dataset_id)
    response = {"data": {"paid_amount_cents": paid}, "meta": meta(latest)}
    cache.put(key, response)
    return response


@app.get("/v1/datasets/{dataset_id}/telemetry/orders")
async def telemetry_orders(
    request: Request,
    dataset_id: DatasetId,
    q: Annotated[SummaryQuery, Query()],
):
    cache = request.app.state.cache
    key = _cache_key(dataset_id, "telemetry")

    if not q.refresh:
        cached = cache.get(key)
        if cached is not None:
            CACHE_REQUESTS.labels(result="hit").inc()
            return cached

    CACHE_REQUESTS.labels(result="miss").inc()

    res = await ch_query(
        request,
        """
        SELECT
            end_to_end_pipeline_latency,
            events
        FROM capstone.telemetry_orders_v
        WHERE dataset_id = {ds:String}
        """,
        {"ds": dataset_id},
    )

    if res.result_rows:
        lag = float(res.result_rows[0][0])
        events = int(res.result_rows[0][1])
    else:
        lag = 0.0
        events = 0

    latest = await latest_event_at(request, dataset_id)

    response = {
        "data": {
            "end_to_end_pipeline_latency_seconds": lag,
            "events": events,
        },
        "meta": meta(latest),
    }

    cache.put(key, response)
    return response


@app.get("/v1/datasets/{dataset_id}/orders")
async def orders(request: Request, dataset_id: Annotated[str, Path(pattern=ID_PATTERN)], filters: Annotated[OrderFilters, Query()],):
    sql = """
        SELECT order_id, campaign_id, version, status, amount_cents, created_at, ingested_at
        FROM capstone.orders_current
        WHERE dataset_id = {ds:String} AND is_deleted = 0
    """
    params: dict = {"ds": dataset_id, "lim": filters.limit + 1}
    if filters.status is not None:
        sql += " AND status = {status:String}"
        params["status"] = filters.status
    if filters.campaign_id is not None:
        sql += " AND campaign_id = {c:String}"
        params["c"] = filters.campaign_id
    if filters.after_order_id is not None:
        sql += " AND order_id > {after:String}"
        params["after"] = filters.after_order_id
    sql += " ORDER BY order_id LIMIT {lim:UInt32}"

    res = await ch_query(request, sql, params)
    rows = res.result_rows
    has_more = len(rows) > filters.limit
    rows = rows[: filters.limit]
    data = [order_to_dict(r) for r in rows]
    return {
        "data": data,
        "next_after_order_id": data[-1]["order_id"] if has_more and data else None,
    }


@app.get("/v1/datasets/{dataset_id}/orders/{order_id}")
async def order_card(
    request: Request, dataset_id: DatasetId, order_id: OrderId, _: Annotated[NoParams, Query()]
):
    res = await ch_query(
        request,
        """
        SELECT order_id, campaign_id, version, status, amount_cents, created_at, ingested_at
        FROM capstone.orders_current
        WHERE dataset_id = {ds:String} AND order_id = {oid:String} AND is_deleted = 0
        LIMIT 1
        """,
        {"ds": dataset_id, "oid": order_id},
    )
    if not res.result_rows:
        raise HTTPException(404, "order not found")
    return {"data": order_to_dict(res.result_rows[0])}
