import asyncio
import os
from fastapi import FastAPI, HTTPException, Query
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from common.observability import configure_otel, instrument_fastapi, setup_logging

SERVICE_NAME = os.getenv("SERVICE_NAME", "airflow-service")
configure_otel(SERVICE_NAME)
logger = setup_logging(SERVICE_NAME)

app = FastAPI(title=SERVICE_NAME)
instrument_fastapi(app)

tracer = trace.get_tracer(__name__)


@app.get("/work")
async def work(
    slow_ms: int = Query(0, ge=0, le=10_000),
    fail: str = Query("none", description="none|exception|http500"),
):
    with tracer.start_as_current_span("c.work") as span:
        span.set_attribute("demo.slow_ms", slow_ms)
        span.set_attribute("demo.fail", fail)

        logger.info(f"/work slow_ms={slow_ms} fail={fail}")

        if slow_ms:
            await asyncio.sleep(slow_ms / 1000.0)

        if fail == "exception":
            try:
                1 / 0
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "forced exception"))
                logger.exception("forced exception")
                raise

        if fail == "http500":
            span.set_status(Status(StatusCode.ERROR, "forced http500"))
            logger.error("forced http500")
            raise HTTPException(status_code=500, detail="service-c forced 500")

        return {"service": SERVICE_NAME, "ok": True, "slow_ms": slow_ms, "fail": fail}