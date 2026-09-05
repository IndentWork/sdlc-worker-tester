"""
Storage service — writes files to the sdlc container.

All blobs follow the hierarchy:
  sdlc/{resource_code}/{github_org}/{type}/...

Examples:
  sdlc/b310545b/sdlc-tenant/hello.txt
  sdlc/b310545b/sdlc-tenant/sdlc.yml

Authentication uses DefaultAzureCredential (Managed Identity in Azure).
"""
import os

from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient

CONTAINER = "sdlc"


def _account_url(tier: str, resource_code: str) -> str:
    """Derive Storage account URL from tenant tier and resource_code."""
    env = os.environ.get("ENV", "dev")
    if tier == "shared":
        name = f"stsdlcshared{env}"
    else:
        name = f"stsdlc{resource_code}{env}"
    return f"https://{name}.blob.core.windows.net"


async def write_hello(tier: str, resource_code: str, github_org: str) -> None:
    """Write hello.txt to sdlc/{resource_code}/{github_org}/hello.txt"""
    url = _account_url(tier, resource_code)
    credential = DefaultAzureCredential()

    async with BlobServiceClient(url, credential) as client:
        blob = client.get_blob_client(CONTAINER, f"{resource_code}/{github_org}/hello.txt")
        await blob.upload_blob(
            b"Hello from sdlc-worker-tester!",
            overwrite=True,
        )


async def write_sdlc_yml(tier: str, resource_code: str, github_org: str, content: str) -> None:
    """Save raw YAML to sdlc/{resource_code}/{github_org}/sdlc.yml"""
    url = _account_url(tier, resource_code)
    credential = DefaultAzureCredential()

    async with BlobServiceClient(url, credential) as client:
        blob = client.get_blob_client(CONTAINER, f"{resource_code}/{github_org}/sdlc.yml")
        await blob.upload_blob(
            content.encode("utf-8"),
            overwrite=True,
            content_type="application/x-yaml",
        )
