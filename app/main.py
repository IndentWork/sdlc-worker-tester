"""
Tester worker — listens to the tester subscription on sdlc-events topic.

Creates the tester subscription on first run with SQL filter for test actions.
Processes test messages:
- action=test_storage: writes hello.txt to tenant's Storage configs container
- action=upload_sdlc: saves raw YAML to configs/{resource_code}/sdlc.yml

Service Bus routes messages via SQL filter on the action application property.
This validates the full end-to-end path: Topic → Subscription → Worker → Storage.

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

TOPIC_NAME = "sdlc-events"
SUBSCRIPTION_NAME = "tester"


async def _process_message(raw: str) -> None:
    """Dispatch a single message to the correct handler based on action field."""
    payload = json.loads(raw)
    action        = payload.get("action")
    resource_code = payload.get("resource_code")
    github_org    = payload.get("github_org", "")
    tier          = payload.get("tier")

    log.info(json.dumps({
        "event":         "message_received",
        "action":        action,
        "tier":          tier,
        "resource_code": resource_code,
    }))

    if action == "test_storage":
        await write_hello(tier, resource_code, github_org)
        log.info(json.dumps({
            "event":         "hello_txt_written",
            "resource_code": resource_code,
            "path":          f"sdlc/{resource_code}/{github_org}/hello.txt",
        }))

    elif action == "upload_sdlc":
        content = payload.get("content", "")
        await write_sdlc_yml(tier, resource_code, github_org, content)
        log.info(json.dumps({
            "event":         "sdlc_yml_saved",
            "resource_code": resource_code,
            "path":          f"sdlc/{resource_code}/{github_org}/sdlc.yml",
        }))

    else:
        log.warning(json.dumps({
            "event":  "unknown_action",
            "action": action,
        }))


async def listen() -> None:
    """
    Long-running loop — receives messages from tester subscription.
    Creates the subscription on first run with SQL filter if it doesn't exist.
    Completes the message on success so it is removed from the subscription.
    Abandons on failure so it returns to the subscription for retry.
    """
    namespace = os.environ["SERVICEBUS_NAMESPACE"]
    credential = DefaultAzureCredential()

    log.info(json.dumps({
        "event":          "worker_started",
        "namespace":      namespace,
        "topic":          TOPIC_NAME,
        "subscription":   SUBSCRIPTION_NAME,
    }))

    async with ServiceBusClient(namespace, credential) as client:
        # Create subscription on first run with SQL filter
        # If subscription already exists, this is a no-op (exists_ok=True)
        try:
            await client.create_subscription(
                TOPIC_NAME,
                SUBSCRIPTION_NAME,
                sql_filter="action = 'test_storage' OR action = 'upload_sdlc'",
                exists_ok=True
            )
            log.info(json.dumps({
                "event": "subscription_created_or_exists",
                "subscription": SUBSCRIPTION_NAME,
                "filter": "action = 'test_storage' OR action = 'upload_sdlc'"
            }))
        except Exception as exc:
            log.warning(json.dumps({
                "event": "subscription_creation_warning",
                "error": str(exc),
                "type": type(exc).__name__,
            }))

        # Listen for messages on the subscription
        async with client.get_subscription_receiver(TOPIC_NAME, SUBSCRIPTION_NAME) as receiver:
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
