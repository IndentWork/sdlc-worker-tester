"""
Tester worker — listens to the repo-index Service Bus queue and processes test messages.

For action=test_storage: writes hello.txt to the tenant's Storage configs container.
This validates the full end-to-end path: Service Bus → worker → Storage.

Environment variables required:
  SERVICEBUS_NAMESPACE  — e.g. sb-sdlc-shared-dev.servicebus.windows.net
  AZURE_CLIENT_ID       — Managed Identity client ID
  ENV                   — dev or prod
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone


# Structured JSON logger — each log line is a JSON object queryable in Azure Monitor.
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    return logging.getLogger("worker.tester")


log = _setup_logging()

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient

from app.services.storage import write_hello, write_sdlc_yml

QUEUE_NAME = "repo-index"


async def _process_message(raw: str) -> None:
    """Dispatch a single message to the correct handler based on action field."""
    payload = json.loads(raw)
    action      = payload.get("action")
    tenant_id   = payload.get("tenant_id")
    tier        = payload.get("tier")
    resource_code = payload.get("resource_code")

    log.info(json.dumps({
        "event":         "message_received",
        "action":        action,
        "tenant_id":     tenant_id,
        "tier":          tier,
        "resource_code": resource_code,
    }))

    if action == "test_storage":
        await write_hello(tier, resource_code)
        log.info(json.dumps({
            "event":         "hello_txt_written",
            "resource_code": resource_code,
        }))

    elif action == "upload_sdlc":
        content = payload.get("content", "")
        await write_sdlc_yml(tier, resource_code, content)
        log.info(json.dumps({
            "event":         "sdlc_yml_saved",
            "resource_code": resource_code,
            "path":          f"configs/{resource_code}/sdlc.yml",
        }))

    else:
        log.warning(json.dumps({
            "event":  "unknown_action",
            "action": action,
        }))


async def listen() -> None:
    """
    Long-running loop — receives messages from repo-index queue one at a time.
    Completes the message on success so it is removed from the queue.
    Abandons on failure so it returns to the queue for retry.
    """
    namespace = os.environ["SERVICEBUS_NAMESPACE"]
    credential = DefaultAzureCredential()

    log.info(json.dumps({
        "event":     "worker_started",
        "namespace": namespace,
        "queue":     QUEUE_NAME,
    }))

    async with ServiceBusClient(namespace, credential) as client:
        async with client.get_queue_receiver(QUEUE_NAME) as receiver:
            async for message in receiver:
                try:
                    await _process_message(str(message))
                    await receiver.complete_message(message)
                except Exception as exc:
                    log.error(json.dumps({
                        "event": "message_failed",
                        "error": str(exc),
                        "type":  type(exc).__name__,
                    }))
                    await receiver.abandon_message(message)


if __name__ == "__main__":
    asyncio.run(listen())
