#!/usr/bin/env python3
"""Python side of the ESP32 <-> Python peering demo.

Runs a real `laila` policy and exposes a tiny TCP JSON-RPC adapter that the
ESP32 (laila-C) connects to. When the ESP32 calls
`laila.request(peer, "central.memory.remember", [gid])`, this server resolves
the entry from the local laila policy and returns it.

  ESP32 (laila-C)  --tcp/json-rpc-->  this adapter  -->  laila.remember(...)

Run:  python python_policy.py            # listens on 0.0.0.0:8770

NOTE on wire format: this minimal example uses a plain-TCP JSON-RPC adapter.
Native WebSocket interop with an UNMODIFIED `laila` is now supported directly:
laila-C speaks RFC6455 + laila's `peer.connect`/`rpc.call`, so a real `laila`
policy can `laila.add_peer("ws://<device>:5556", secret)` and call
memorize/remember/forget with no adapter. See `tests/interop/run_interop.py`
(host + `--qemu-uri` device legs) and the "Python <-> laila-C interop" section
of the top-level laila-C README; this file is kept as a tiny standalone demo.
"""
import json
import socketserver

import laila

HOST, PORT = "0.0.0.0", 8770

# Seed the policy with an entry the ESP32 will ask for, addressed by nickname.
_seed = laila.constant(data="calib:gain=1.07,offset=-3", nickname="sensor_calibration")
laila.memorize(_seed)


def entry_to_laila_c_dict(entry) -> dict:
    """Format a laila Entry in laila-C's on-disk schema (tagged payload)."""
    value = entry.data
    if isinstance(value, str):
        payload = {"k": "string", "v": value}
    elif isinstance(value, bool):
        payload = {"k": "bool", "v": value}
    elif isinstance(value, int):
        payload = {"k": "int", "v": value}
    elif isinstance(value, float):
        payload = {"k": "double", "v": value}
    else:
        payload = {"k": "json", "v": value}
    return {
        "_uuid": entry.uuid,
        "_evolution": entry.evolution,
        "_scopes": ["ENTRY"],
        "_state": "READY",
        "payload": payload,
        "constitution": None,
    }


def handle_rpc(req: dict) -> dict:
    params = req.get("params", {})
    path = params.get("path", [])
    args = params.get("args", [])
    verb = path[-1] if path else ""
    try:
        if verb == "remember":
            entry = laila.remember(args[0])  # Future
            laila.wait(entry)
            return {"jsonrpc": "2.0", "id": req.get("id"), "result": entry_to_laila_c_dict(entry.result)}
        if verb == "forget":
            laila.wait(laila.forget(args[0]))
            return {"jsonrpc": "2.0", "id": req.get("id"), "result": None}
        return {"jsonrpc": "2.0", "id": req.get("id"),
                "error": {"code": -32601, "message": f"method not supported: {verb}"}}
    except Exception as exc:  # noqa: BLE001
        return {"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32000, "message": str(exc)}}


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(65536)
        if not data:
            return
        req = json.loads(data.decode("utf-8"))
        resp = handle_rpc(req)
        self.request.sendall(json.dumps(resp).encode("utf-8"))


if __name__ == "__main__":
    print(f"[python] laila policy serving peers on {HOST}:{PORT}")
    with socketserver.TCPServer((HOST, PORT), Handler) as srv:
        srv.serve_forever()
