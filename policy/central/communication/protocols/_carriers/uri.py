"""Small URI parsing helpers shared by concrete transports.

Transports peer via ``add_peer(uri, secret)``; these helpers turn the
common URI shapes (``scheme://host:port``, ``scheme:///path``) into the
address components a driver needs, without each transport re-deriving
the same ``urllib.parse`` boilerplate.
"""

from __future__ import annotations

from urllib.parse import urlparse


def split_host_port(uri: str, default_host: str = "127.0.0.1", default_port: int = 0) -> tuple[str, int]:
    """Return ``(host, port)`` from a ``scheme://host:port`` URI."""
    parsed = urlparse(uri)
    host = parsed.hostname or default_host
    port = parsed.port if parsed.port is not None else default_port
    return host, port


def uri_path(uri: str) -> str:
    """Return the path component of a ``scheme:///path`` URI."""
    parsed = urlparse(uri)
    # netloc-less URIs (unix:///tmp/x) keep the leading slash in path.
    return parsed.path or parsed.netloc


def uri_authority(uri: str) -> str:
    """Return the authority (everything after ``scheme://``) verbatim."""
    parsed = urlparse(uri)
    return parsed.netloc or parsed.path.lstrip("/")
