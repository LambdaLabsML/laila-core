#!/usr/bin/env bash
# QEMU networking smoke test: boot laila-C on an emulated ESP32, then pull an
# entry FROM the device over real TCP (host -> emulated device), proving the
# networked inbound RPC server end-to-end on-target.
#
# Prereqs: ESP-IDF env sourced (. $IDF_PATH/export.sh) and a host (POSIX) build
# of laila-C at ../build (cmake -S .. -B ../build && cmake --build ../build).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
HOST_PORT="${HOST_PORT:-15556}"
cd "$HERE"

command -v idf.py >/dev/null || { echo "FAIL: idf.py not on PATH (source export.sh)"; exit 2; }

# Host POSIX libs (for the client). Build them if absent.
if [ ! -f "$ROOT/build/liblaila_core.a" ]; then
  cmake -S "$ROOT" -B "$ROOT/build" >/dev/null
  cmake --build "$ROOT/build" -j >/dev/null
fi

echo "[qemu-test] building firmware..."
idf.py build >/dev/null

echo "[qemu-test] building host client..."
g++ -std=c++17 -I"$ROOT/include" -I"$ROOT/hal/include" host_client.cpp \
  -Wl,--start-group "$ROOT/build/liblaila_core.a" \
  "$ROOT/build/platform/posix/liblaila_hal_posix.a" -Wl,--end-group \
  -pthread -o /tmp/laila_host_client

LOG="$(mktemp)"
echo "[qemu-test] booting QEMU (host :$HOST_PORT -> guest :5556)..."
nohup idf.py qemu --qemu-extra-args \
  "-nic user,model=open_eth,hostfwd=tcp::${HOST_PORT}-:5556" > "$LOG" 2>&1 &
cleanup() { pkill -f qemu-system-xtensa 2>/dev/null || true; }
trap cleanup EXIT

for _ in $(seq 1 40); do grep -aq "LAILA_SERVE_READY" "$LOG" && break; sleep 2; done
if ! grep -aq "LAILA_SERVE_READY" "$LOG"; then
  echo "FAIL: device server never became ready"; tail -40 "$LOG"; exit 1
fi

GID="$(grep -a "LAILA_ENTRY_GID" "$LOG" | tail -1 | awk '{print $2}' | tr -d '\r\n')"
echo "[qemu-test] pulling $GID from emulated device..."
OUT="$(/tmp/laila_host_client "$GID" "$HOST_PORT" | tr -d '\r')"
echo "  device returned: $OUT"

if [ "$OUT" = "HOST_GOT=sensor=42" ]; then
  echo "QEMU_NET_TEST: PASS"
else
  echo "QEMU_NET_TEST: FAIL (got '$OUT')"; exit 1
fi

# ---- Python <-> emulated device interop (unmodified Python laila over WebSocket) ----
# An unmodified Python `laila` policy peers with the device via ws:// (the device
# listener auto-detects the RFC6455 upgrade) and runs memorize / remember / forget.
# ESP-IDF's export.sh puts its own venv python first; that venv usually lacks
# `laila`/`websockets`, so probe a few interpreters for one that has both.
PKG_PARENT="$(cd "$ROOT/../.." && pwd)"   # parent of the `laila` package
PY=""
for cand in "${LAILA_PY:-}" /usr/bin/python3 python3; do
  [ -n "$cand" ] && command -v "$cand" >/dev/null 2>&1 || continue
  if "$cand" -c "import sys; sys.path.insert(0, '$PKG_PARENT'); import laila, websockets" 2>/dev/null; then
    PY="$cand"; break
  fi
done
if [ -n "$PY" ]; then
  echo "[qemu-test] Python ($PY) laila <-> device interop over ws://127.0.0.1:$HOST_PORT ..."
  if PYTHONPATH="$PKG_PARENT:${PYTHONPATH:-}" "$PY" "$ROOT/tests/interop/run_interop.py" \
       --device-only --qemu-uri "tcp://127.0.0.1:${HOST_PORT}" --device-gid "$GID"; then
    echo "QEMU_PY_INTEROP: PASS"
  else
    echo "QEMU_PY_INTEROP: FAIL"; exit 1
  fi
else
  echo "QEMU_PY_INTEROP: SKIP (no python3 with laila+websockets found)"
fi
