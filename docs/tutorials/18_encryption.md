# Tutorial 18: End-to-End Encrypted Entries

LAILA's pool transformations are composable: a pool serializes through a `TransformationSequence`, and you can drop a `FernetEncryption` step into that sequence to get at-rest encryption for any backend.

## Prerequisites

```bash
pip install "laila-core[crypto]"
```

## Generate a key

Fernet keys are 32 URL-safe base64-encoded bytes. In production you would load this from a key manager — for the tutorial we generate one and keep it in memory:

```python
from cryptography.fernet import Fernet
key = Fernet.generate_key()
```

## Build an encrypted pool

`transformation_base64_compression_encryption(key)` returns a pre-built sequence that compresses, encrypts, then base64-encodes. Pass it to any pool via the `transformations` field — the same recipe works for S3, HDF5, SQLite, or any other backend:

```python
import laila
from laila.entry import transformation_base64_compression_encryption
from laila.pool import FilesystemPool

vault = FilesystemPool(
    nickname="vault",
    transformations=transformation_base64_compression_encryption(key),
)
laila.memory.extend(vault, pool_nickname="vault")
```

## Memorize a secret

```python
secret = laila.constant(
    data={"username": "alice", "api_key": "sk-live-very-secret"},
    nickname="prod_credentials",
)
laila.memorize(secret, pool_nickname="vault").wait()
```

## Inspect raw on-disk bytes

The filesystem pool stores blobs as files under its image directory. Open one directly and you'll see only ciphertext — no plaintext traces of "alice" or the API key:

```python
from pathlib import Path
for f in Path(vault._mount_dir).rglob("*"):
    if f.is_file():
        head = f.read_bytes()[:96]
        assert b"alice" not in head
        assert b"sk-live" not in head
```

## Recall and verify

Reading through `laila.remember` runs the transformations in reverse — base64 decode, decrypt, decompress, deserialize — and the original payload comes back intact:

```python
recovered = laila.remember(nickname="prod_credentials", pool_nickname="vault", persist=False).wait()
print(recovered.data)
# {'username': 'alice', 'api_key': 'sk-live-very-secret'}
```

## Operational notes

| Topic | Note |
|---|---|
| Key rotation | Re-encrypt by reading with the old key pool and writing to a new pool built with the new key. |
| Key storage | Use a real KMS or the `secrets/` subdirectory under `set_default_directory`. |
| TTLs | `FernetEncryption.backward_kwargs = {"ttl": seconds}` rejects tokens older than the cutoff. |
| Layering | Drop the encryption step into any `TransformationSequence` — it composes with compression, base64, and serializers freely. |

## Summary

- `FernetEncryption` is one transformation step among many.
- `transformation_base64_compression_encryption(key)` is the ready-made sequence for compact, encrypted blobs.
- Decryption happens transparently inside `remember` — your code never sees ciphertext.

Next: [Tutorial 19 — Object Stores Beyond AWS](19_object_stores_beyond_aws.md).
