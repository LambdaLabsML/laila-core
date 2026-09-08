# ESP32 ↔ Python peer request (`laila.request`)

The ESP32 runs a laila-C policy and treats a Python machine's `laila` policy as
a **peer**. It calls `laila->request(...)` to invoke a method on the Python
policy over the network and get a `Future` back — the device-side equivalent of:

```python
# Python form
peer  = laila.add_peer("tcp://192.168.1.50:8770", secret="s3cr3t")
value = laila.request(peer, "central.memory.remember", [gid]).data
```

```cpp
// laila-C form (only . -> -> differs)
auto peer  = laila->add_peer("tcp://192.168.1.50:8770", "s3cr3t");
auto value = laila->request(peer, "central.memory.remember", {gid})->data();
// or the convenience verb:
auto value = laila->peer(peer)->remember(gid)->data();
```

## Flow

```
ESP32 (laila-C policy)                Python machine (laila policy)
  laila->add_peer("tcp://host:8770")  --TCP connect-->  JSON-RPC adapter
  laila->request(peer, "central.memory.remember", [gid])
        --- {"method":"rpc.call","params":{"path":["central","memory","remember"],
             "args":[gid]}} --->
                                       laila.remember(gid).data
        <--- {"result": <entry dict>} ---
  future->data()  ==  the value held by the Python policy
```

## Run the demo (host)

```bash
# Terminal 1 — Python side (needs the laila package importable):
python examples/esp32_peer_request/python_policy.py     # serves on :8770

# Terminal 2 — the laila-C client (built as example_esp32_peer_request):
./build/examples/example_esp32_peer_request              # connects to 127.0.0.1:8770
```

## On a real ESP32

Build with `-DLAILA_PLATFORM=esp32` inside ESP-IDF, bring up Wi-Fi first (see the
`#ifdef ESP_PLATFORM` block in `esp32_main.cpp`), and point
`LAILA_PYTHON_PEER_URI` at your Python host. The ESP32 transport uses the
`esp32` HAL backend's TCP support.

## Note on wire format

laila's native peer transport is WebSocket + JSON-RPC. This minimal demo's
Python adapter speaks the same JSON-RPC method/params shape over plain TCP.

Native WebSocket interop with an **unmodified** `laila` is now implemented:
laila-C is a real RFC6455 client/server speaking laila's `peer.connect` /
`rpc.call`, with cross-language payloads exchanged as laila's native entry dict.
A real `laila` policy can `laila.add_peer("ws://<device>:5556", secret)` and run
memorize/remember/forget directly -- see `tests/interop/run_interop.py` and the
"Python <-> laila-C interop" section of the laila-C README. This example is kept
as the simplest possible standalone bridge.
