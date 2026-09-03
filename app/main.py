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

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient

from app.services.storage import write_hello

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

QUEUE_NAME = "repo-index"


async def _process_message(raw: str) -> None:
    """Dispatch a single message to the correct handler based on action field."""
    payload = json.loads(raw)
    action = payload.get("action")
    tenant_id = payload.get("tenant_id")
    tier = payload.get("tier")
    resource_code = payload.get("resource_code")

    log.info("Received action=%s tenant_id=%s", action, tenant_id)

    if action == "test_storage":
        await write_hello(tier, resource_code, tenant_id)
        log.info("hello.txt written for tenant_id=%s", tenant_id)
    else:
        log.warning("Unknown action=%s — skipping", action)


async def listen() -> None:
    """
    Long-running loop — receives messages from repo-index queue one at a time.
    Completes the message on success so it is removed from the queue.
    Abandons on failure so it returns to the queue and retries.
    """
    namespace = os.environ["SERVICEBUS_NAMESPACE"]
    credential = DefaultAzureCredential()

    log.info("Starting worker on %s / %s", namespace, QUEUE_NAME)

    async with ServiceBusClient(namespace, credential) as client:
        async with client.get_queue_receiver(QUEUE_NAME) as receiver:
            async for message in receiver:
                try:
                    await _process_message(str(message))
                    await receiver.complete_message(message)
                except Exception as exc:
                    log.error("Failed to process message: %s", exc)
                    await receiver.abandon_message(message)


if __name__ == "__main__":
    asyncio.run(listen())
