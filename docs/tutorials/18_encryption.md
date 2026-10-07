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

## Register the key with LAILA

Like every other secret, the key lives in `laila.args`. Set it once per process, on
**both** the writing and the reading side. The canonical location is
`laila.args.encryption.key`; `laila.encryption_key` is a convenience alias:

```python
import laila
laila.encryption_key = key            # same as: laila.args.encryption.key = key
```

A secrets file loaded through `laila.read_args(...)` works too, e.g. a TOML file
containing an `[encryption]` table with `key = "..."`.

The key is never embedded in what gets stored: the recovery code written next to the
ciphertext carries only a short fingerprint of the key, so a reader configured with a
different key gets a clear `ValueError` instead of garbage.

## Build an encrypted pool

`transformation_base64_compression_encryption()` returns a pre-built sequence that compresses, encrypts, then base64-encodes, picking the key up from `laila.encryption_key`. Pass it to any pool via the `transformations` field — the same recipe works for S3, HDF5, SQLite, or any other backend:

```python
from laila.entry import transformation_base64_compression_encryption
from laila.data import FilesystemPool

vault = FilesystemPool(
    nickname="vault",
    transformations=transformation_base64_compression_encryption(),
)
laila.memory.extend(vault, pool_nickname="vault")
```

Passing an explicit `key=` to the factory (or to `FernetEncryption(key=...)`) is still
supported for ad-hoc pipelines; the reading side still resolves the key from its own
`laila.encryption_key`.

## Memorize a secret

```python
secret = laila.constant(
    data={"username": "alice", "api_key": "sk-live-very-secret"},
    nickname="prod_credentials",
)
laila.memorize(secret, dst_pool="vault").wait()
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
recovered = laila.remember(nickname="prod_credentials", dst_pool="vault", persist=False).wait()
print(recovered.data)
# {'username': 'alice', 'api_key': 'sk-live-very-secret'}
```

## Operational notes

| Topic | Note |
|---|---|
| Key rotation | Re-encrypt by reading with the old key pool and writing to a new pool built with the new key. |
| Key storage | Use a real KMS or a file in the `secrets/` subdirectory under `set_default_directory`, loaded with `laila.read_args(...)` into `laila.args.encryption.key`. |
| Key mismatch | Reading with a different `laila.encryption_key` than the writer raises `ValueError` mentioning the writer's key fingerprint. |
| TTLs | `FernetEncryption.backward_kwargs = {"ttl": seconds}` rejects tokens older than the cutoff. |
| Layering | Drop the encryption step into any `TransformationSequence` — it composes with compression, base64, and serializers freely. |

## Summary

- `FernetEncryption` is one transformation step among many.
- `transformation_base64_compression_encryption()` is the ready-made sequence for compact, encrypted blobs; the key comes from `laila.encryption_key` on both sides.
- Decryption happens transparently inside `remember` — your code never sees ciphertext.

Next: [Tutorial 19 — Object Stores Beyond AWS](19_object_stores_beyond_aws.md).
