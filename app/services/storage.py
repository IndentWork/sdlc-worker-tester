"""
Storage service — writes hello.txt to the configs container.

Storage account is derived from the message payload (tier + resource_code + env).
Authentication uses DefaultAzureCredential (Managed Identity in Azure).
"""
import os

from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient


def _account_url(tier: str, resource_code: str) -> str:
    """Derive the Storage account URL from tenant tier and resource_code."""
    env = os.environ.get("ENV", "dev")
    if tier == "shared":
        name = f"stsdlcshared{env}"
    else:
        name = f"stsdlc{resource_code}{env}"
    return f"https://{name}.blob.core.windows.net"


async def write_hello(tier: str, resource_code: str) -> None:
    """Write hello.txt to configs/{resource_code}/hello.txt in the tenant's Storage account."""
    url = _account_url(tier, resource_code)
    credential = DefaultAzureCredential()

    async with BlobServiceClient(url, credential) as client:
        container = client.get_container_client("configs")
        blob = container.get_blob_client(f"{resource_code}/hello.txt")
        await blob.upload_blob(
            b"Hello from sdlc-worker-tester!",
            overwrite=True,
        )


async def write_sdlc_yml(tier: str, resource_code: str, content: str) -> None:
    """Save raw YAML content to configs/{resource_code}/sdlc.yml in the tenant's Storage account."""
    url = _account_url(tier, resource_code)
    credential = DefaultAzureCredential()

    async with BlobServiceClient(url, credential) as client:
        container = client.get_container_client("configs")
        blob = container.get_blob_client(f"{resource_code}/sdlc.yml")
        await blob.upload_blob(
            content.encode("utf-8"),
            overwrite=True,
            content_type="application/x-yaml",
        )
