"""Trust gate for constitutions rebuilt from storage or the wire.

A constitution is Python source that gets ``exec``'d when an entry is
materialised. Entries are rebuilt from pool files and from peer replies,
i.e. from inputs the reading process did not produce. Two gates make
sure only code the reader could have produced itself ever runs:

- **Simple constitutions** (transformation inverse chains) are accepted
  only when every snippet matches the recovery-code *template* of a
  loaded transformation class, or is byte-identical to a snippet this
  process generated (see
  :meth:`_data_transformation.is_trusted_backward_code`). Templates are
  parametrised on ``backward_kwargs`` (a literal dict) and, for Fernet,
  the key fingerprint, so records written by other processes, hosts or
  the JS/C ports still match.
- **Complex constitutions** (user builders) carry an HMAC-SHA256 over
  the canonical ``{code, manifest_global_id}`` pair, keyed by a
  per-user secret. The secret lives in ``~/.laila/constitution.key``
  (created on first use, mode ``0600``); it is deliberately *not*
  derived from ``laila.encryption_key`` so rotating the data-encryption
  key never invalidates stored constitutions, and it is independent of
  ``laila.set_default_directory`` so every process of the same user on
  a host trusts the same code. To let another host (or user) read
  complex constitutions you wrote, copy that file.

Residual risk, documented: ``PickleSerializer`` and ``allow_pickle`` /
``weights_only`` kwargs on the numpy / torch templates remain a
deserialisation vector for hostile pool *payloads* -- that is pickle's
contract, not a constitution hole.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import threading
from pathlib import Path
from typing import Any

_SECRET_ENV = "LAILA_CONSTITUTION_KEY_FILE"
_SECRET_FILENAME = "constitution.key"
_SECRET_NBYTES = 32
_lock = threading.Lock()
_cached_secret: bytes | None = None


def _secret_path() -> Path:
    override = os.environ.get(_SECRET_ENV)
    if override:
        return Path(os.path.expanduser(override))
    return Path.home() / ".laila" / _SECRET_FILENAME


def constitution_secret() -> bytes:
    """Return the per-user HMAC secret, creating it on first use.

    Falls back to a process-local random secret (with a logged warning)
    when the home directory is not writable; constitutions written in
    that mode are only readable by the same process.
    """
    global _cached_secret
    with _lock:
        if _cached_secret is not None:
            return _cached_secret
        path = _secret_path()
        try:
            if path.exists():
                data = path.read_bytes().strip()
                if len(data) >= 16:
                    _cached_secret = data
                    return _cached_secret
            path.parent.mkdir(parents=True, exist_ok=True)
            fresh = secrets.token_hex(_SECRET_NBYTES).encode("ascii")
            tmp = path.with_suffix(".tmp")
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(fresh)
            finally:
                try:
                    os.chmod(str(tmp), 0o600)
                except OSError:
                    pass
            os.replace(str(tmp), str(path))
            # Another process may have raced us: honour whatever won.
            _cached_secret = path.read_bytes().strip()
            return _cached_secret
        except OSError as exc:  # pragma: no cover - unwritable HOME
            try:
                from ...logger import get_logger

                get_logger().warning(
                    f"constitution secret file {path} is not usable ({exc}); using a "
                    "process-local secret -- complex constitutions written now are only "
                    "readable by this process"
                )
            except Exception:
                pass
            _cached_secret = secrets.token_bytes(_SECRET_NBYTES)
            return _cached_secret


def _canonical(code: Any, manifest_global_id: Any) -> bytes:
    return json.dumps(
        {"code": code, "manifest_global_id": manifest_global_id},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sign(payload: bytes) -> str:
    return hmac.new(constitution_secret(), payload, hashlib.sha256).hexdigest()


def _verify(payload: bytes, signature: Any) -> bool:
    if not isinstance(signature, str) or not signature:
        return False
    return hmac.compare_digest(_sign(payload), signature)


def sign_complex(code: Any, manifest_global_id: Any) -> str:
    """HMAC-SHA256 (hex) over the canonical complex-constitution payload."""
    return _sign(_canonical(code, manifest_global_id))


def verify_complex(code: Any, manifest_global_id: Any, signature: Any) -> bool:
    """Constant-time check of *signature* against the local secret."""
    return _verify(_canonical(code, manifest_global_id), signature)


def _canonical_codes(codes: Any) -> bytes:
    return json.dumps(list(codes or ()), separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_simple(codes: Any) -> str:
    """HMAC-SHA256 (hex) over a simple constitution's code list."""
    return _sign(_canonical_codes(codes))


def check_simple_codes(codes: Any, signature: Any = None) -> None:
    """Raise ``ValueError`` unless *codes* may run in this process.

    Accepted when either every snippet matches a loaded transformation's
    recovery-code template (or was generated here) -- the normal case
    for pipeline-emitted constitutions, including ones written by other
    hosts and by the JS/C ports -- or the list as a whole carries a
    valid ``_hmac`` from a writer sharing this user's secret (hand-written
    simple constitutions).
    """
    from ..compdata.transformation.base import _data_transformation

    if signature is not None and _verify(_canonical_codes(codes), signature):
        return
    for code in codes or ():
        if not _data_transformation.is_trusted_backward_code(code):
            head = (code if isinstance(code, str) else repr(code)).strip().splitlines()[:1]
            raise ValueError(
                "untrusted constitution: recovery code does not match any loaded "
                f"transformation template and was not generated by this process ({head!r})"
            )
