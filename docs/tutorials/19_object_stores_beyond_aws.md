# Tutorial 19: Object Stores Beyond AWS

LAILA's S3 adapter is the most commonly seen one, but the same `memorize` / `remember` / `forget` API targets Google Cloud Storage, Azure Blob Storage, Cloudflare R2, and Backblaze B2 with one-line constructor changes. This tutorial walks through each.

## Prerequisites

```bash
pip install "laila-core[gcs,azure,cloudflare,backblaze]"
```

Plus credentials for whichever provider you want to try. The notebook wraps each provider in try/except so missing credentials skip cleanly.

## Credentials in `secrets.toml`

```toml
GCS_PROJECT_ID = "my-gcp-project"
GCS_BUCKET = "my-gcs-bucket"
GCS_SERVICE_ACCOUNT_JSON = "/path/to/service-account.json"

AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=..."
AZURE_CONTAINER = "my-container"

R2_ACCOUNT_ID = "..."
R2_ACCESS_KEY_ID = "..."
R2_SECRET_ACCESS_KEY = "..."
R2_BUCKET = "my-r2-bucket"

B2_APP_KEY_ID = "..."
B2_APP_KEY = "..."
B2_BUCKET = "my-b2-bucket"
```

```python
import laila
laila.read_args("./secrets.toml")
```

## Google Cloud Storage

```python
from laila.pool import GCSPool
import json

sa_info = json.loads(open(laila.args.GCS_SERVICE_ACCOUNT_JSON).read())
gcs = GCSPool(
    service_account_info=sa_info,
    project_id=laila.args.GCS_PROJECT_ID,
    bucket_name=laila.args.GCS_BUCKET,
    nickname="gcs",
)
laila.memory.extend(gcs, pool_nickname="gcs")
```

## Azure Blob Storage

```python
from laila.pool import AzurePool

azure = AzurePool(
    connection_string=laila.args.AZURE_CONNECTION_STRING,
    container_name=laila.args.AZURE_CONTAINER,
    nickname="azure",
)
laila.memory.extend(azure, pool_nickname="azure")
```

## Cloudflare R2

R2 speaks the S3 protocol. `CloudflarePool` is a thin wrapper that fills in the endpoint URL from your account id:

```python
from laila.pool import CloudflarePool

r2 = CloudflarePool(
    account_id=laila.args.R2_ACCOUNT_ID,
    access_key_id=laila.args.R2_ACCESS_KEY_ID,
    secret_access_key=laila.args.R2_SECRET_ACCESS_KEY,
    bucket_name=laila.args.R2_BUCKET,
    nickname="r2",
)
laila.memory.extend(r2, pool_nickname="r2")
```

## Backblaze B2

B2 also speaks S3-compatible; the constructor takes application keys instead of access keys:

```python
from laila.pool import BackblazePool

b2 = BackblazePool(
    application_key_id=laila.args.B2_APP_KEY_ID,
    application_key=laila.args.B2_APP_KEY,
    bucket_name=laila.args.B2_BUCKET,
    nickname="b2",
)
laila.memory.extend(b2, pool_nickname="b2")
```

## Picking a provider

| Provider | Strengths | Notes |
|---|---|---|
| AWS S3 | Mature ecosystem, broadest region coverage | The default in Tutorials 3+ |
| GCS | Native integration with GCP services, strong throughput | Service-account JSON is the standard credential |
| Azure Blob | Tight Azure / Entra integration | Connection strings collapse host + key into one value |
| Cloudflare R2 | Zero egress fees within Cloudflare's network | S3-compatible; works with most existing tooling |
| Backblaze B2 | Cheapest storage tier among major providers | S3-compatible; pair with B2 native API for lifecycle rules |

## Summary

- The body of every `memorize` / `remember` call is identical across providers — only the constructor changes.
- Credentials flow through `laila.args` via `read_args`, so a `secrets.toml` works for all providers at once.
- Wrap each provider cell in try/except so missing credentials skip gracefully.

Next: [Tutorial 20 — Structured Backends](20_structured_backends.md).
