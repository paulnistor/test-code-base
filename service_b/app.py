import os
import httpx
from fastapi import FastAPI, Query
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from common.observability import configure_otel, instrument_fastapi, setup_logging

SERVICE_NAME = os.getenv("SERVICE_NAME", "lambda-service")
SERVICE_C_URL = os.getenv("SERVICE_C_URL", "http://localhost:8003").rstrip("/")

configure_otel(SERVICE_NAME)
logger = setup_logging(SERVICE_NAME)

app = FastAPI(title=SERVICE_NAME)
instrument_fastapi(app)

tracer = trace.get_tracer(__name__)


@app.get("/process")
async def process(
    job_id: str,
    slow_ms: int = Query(0, ge=0, le=10_000),
    fail: str = Query("none", description="none|exception|http500"),
    b_fail: str = Query("none", description="none|open_timeout"),
):
    """
    Calls service-c. b_fail can simulate failure before reaching C.
    """
    with tracer.start_as_current_span("b.process") as span:
        span.set_attribute("demo.job_id", job_id)
        span.set_attribute("demo.b_fail", b_fail)

        logger.info(f"/process job_id={job_id} -> C={SERVICE_C_URL}")

        if b_fail == "open_timeout":
            span.set_status(Status(StatusCode.ERROR, "simulated connect timeout"))
            raise httpx.ConnectTimeout("simulated connect timeout in service-b")

        url = f"{SERVICE_C_URL}/work"
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                r = await client.get(url, params={"slow_ms": slow_ms, "fail": fail})
                r.raise_for_status()
                return {"service": SERVICE_NAME, "job_id": job_id, "downstream": r.json()}
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "downstream failed"))
                logger.exception("call to service-c failed")
                raise