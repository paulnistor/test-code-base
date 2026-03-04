import logging
import os
from typing import Dict

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def _parse_headers(raw: str) -> Dict[str, str]:
    # "k=v,k2=v2" -> dict
    raw = (raw or "").strip()
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


class TraceContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        span = trace.get_current_span()
        ctx = span.get_span_context() if span else None
        record.trace_id = f"{ctx.trace_id:032x}" if ctx and ctx.trace_id else "-"
        record.span_id = f"{ctx.span_id:016x}" if ctx and ctx.span_id else "-"
        return True


def setup_logging(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s "
            f"service={service_name} trace_id=%(trace_id)s span_id=%(span_id)s "
            "%(message)s"
        )
    )
    handler.addFilter(TraceContextFilter())

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def configure_otel(service_name: str) -> None:
    """
    Send OTLP/HTTP to your Collector.
    Set OTEL_EXPORTER_OTLP_ENDPOINT to the collector OTLP HTTP endpoint base,
    typically http://localhost:4318 (or your container DNS name).
    """
    base = "http://localhost:4318"

    traces_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", f"{base}/v1/traces")
    metrics_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", f"{base}/v1/metrics")

    resource = Resource.create(
        {
            "service.name": service_name,
            "deployment.environment": os.getenv("DEPLOYMENT_ENV", "local"),
        }
    )

    # Traces
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
    trace.set_tracer_provider(tp)

    # Metrics (optional, but handy)
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=metrics_endpoint),
        export_interval_millis=5000,
    )
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)

    # Critical: instruments httpx so trace headers propagate on outbound calls
    HTTPXClientInstrumentor().instrument()


def instrument_fastapi(app) -> None:
    FastAPIInstrumentor.instrument_app(app)