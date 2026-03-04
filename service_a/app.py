import os
import uuid
import httpx
from fastapi import FastAPI, Query
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from common.observability import configure_otel, instrument_fastapi, setup_logging

SERVICE_NAME = os.getenv("SERVICE_NAME", "post-service")
SERVICE_B_URL = os.getenv("SERVICE_B_URL", "http://localhost:8002").rstrip("/")

configure_otel(SERVICE_NAME)
logger = setup_logging(SERVICE_NAME)

app = FastAPI(title=SERVICE_NAME)
instrument_fastapi(app)

tracer = trace.get_tracer(__name__)


@app.get("/start")
async def start(
    user: str = Query("paul"),
    slow_ms: int = Query(0, ge=0, le=10_000),
    fail: str = Query("none", description="none|exception|http500"),
    b_fail: str = Query("none", description="none|open_timeout"),
    tolerate_b_failure: bool = Query(True),
):
    """
    Entry point: A -> B -> C.
    tolerate_b_failure lets you see traces where the HTTP response is 200 but spans show an error.
    """
    job_id = str(uuid.uuid4())

    with tracer.start_as_current_span("a.start") as span:
        span.set_attribute("demo.user", user)
        span.set_attribute("demo.job_id", job_id)

        logger.info(f"/start user={user} job_id={job_id} -> B={SERVICE_B_URL}")

        url = f"{SERVICE_B_URL}/process"
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                r = await client.get(
                    url,
                    params={"job_id": job_id, "slow_ms": slow_ms, "fail": fail, "b_fail": b_fail},
                )
                r.raise_for_status()
                return {"service": SERVICE_NAME, "job_id": job_id, "user": user, "result": r.json()}
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "service-b failed"))
                logger.exception("service-b failed")

                if tolerate_b_failure:
                    return {"service": SERVICE_NAME, "job_id": job_id, "user": user, "partial": True, "error": str(e)}
                raise