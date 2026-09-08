#!/usr/bin/env bash
# Build laila-C (host) and run the Python <-> laila-C interop matrix over a real
# WebSocket connection: an unmodified Python `laila` policy peers with laila-C
# policies and exchanges memorize / remember / forget bidirectionally and
# tri-directionally. Pass --qemu-uri ws://host:port to also drive an emulated
# ESP32 device.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"     # laila-c/
PKG="$(cd "$ROOT/.." && pwd)"         # the `laila` Python package dir
PARENT="$(cd "$PKG/.." && pwd)"       # its parent, so `import laila` resolves

echo "[interop] building laila-C host harness..."
cmake -S "$ROOT" -B "$ROOT/build" >/dev/null
cmake --build "$ROOT/build" -j --target c_harness >/dev/null

export C_HARNESS="$ROOT/build/tests/c_harness"
export PYTHONPATH="$PARENT:${PYTHONPATH:-}"
echo "[interop] running Python<->C matrix (C_HARNESS=$C_HARNESS)"
python3 "$HERE/run_interop.py" "$@"
