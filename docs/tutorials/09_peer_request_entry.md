# Tutorial 9: Peer-to-Peer Communication on Localhost

LAILA policies can **peer** over TCP so that each node transparently operates on the other's memory. In this tutorial two policies run on `127.0.0.1` in **separate processes** — the remote peer is a subprocess that shares nothing with the notebook except the TCP link. After peering, either side can store and retrieve entries that physically live on the other node.

## Prerequisites

```bash
pip install "laila-core"
```

No credentials or external services required.

## Setup

```python
import sys
import subprocess
import textwrap
import time
import laila
from laila.macros.defaults import DefaultPolicy, DefaultPool, DefaultTCPIPProtocol
```

## Step 1: Launch the remote peer as a subprocess

The remote process creates its own `DefaultPolicy`, registers a pool, stores an entry, and opens a TCP listener on a random port. It prints its `PORT`, `SECRET`, and `ENTRY_ID` to stdout so the notebook can connect to it.

After printing `READY` the subprocess waits for the main process to peer with it. Once peered, it reads the main's entry ID and pool nickname from stdin, requests that entry via the peer proxy, and prints the result — demonstrating the reverse direction.

```python
REMOTE_SCRIPT = textwrap.dedent("""\
    import sys, time, uuid, laila
    from laila.macros.defaults import DefaultPolicy, DefaultPool, DefaultTCPIPProtocol

    policy = DefaultPolicy()
    pool = DefaultPool()
    laila.memory.extend(pool=pool, pool_nickname="remote-store")

    entry = laila.constant(
        data={"message": "Hello from the remote process"},
        nickname="remote-entry",
    )
    with laila.guarantee:
        laila.memorize(entries=entry, pool_nickname="remote-store")

    tcp = DefaultTCPIPProtocol(
        host="127.0.0.1",
        port=0,
        peer_secret_key=uuid.uuid4().hex,
    )
    laila.communication.add_connection(tcp)

    print(f"PORT={tcp.port}", flush=True)
    print(f"SECRET={tcp.peer_secret_key}", flush=True)
    print(f"ENTRY_ID={entry.global_id}", flush=True)
    print("READY", flush=True)

    while not laila.peers:
        time.sleep(0.2)
    time.sleep(0.3)

    main_entry_id = sys.stdin.readline().strip()
    main_pool_nickname = sys.stdin.readline().strip()

    peer_id = list(laila.peers.keys())[0]
    proxy = laila.peers[peer_id]
    laila.active_policy = proxy
    with laila.guarantee:
        result = laila.remember(entry_ids=main_entry_id, pool_nickname=main_pool_nickname)

    print(f"REMOTE_RETRIEVED={result.data[0]}", flush=True)

    while True:
        time.sleep(1)
""")

proc = subprocess.Popen(
    [sys.executable, "-c", REMOTE_SCRIPT],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
)

remote_port = None
remote_secret = None
remote_entry_id = None
for line in proc.stdout:
    line = line.strip()
    if line.startswith("PORT="):
        remote_port = int(line.split("=", 1)[1])
    elif line.startswith("SECRET="):
        remote_secret = line.split("=", 1)[1]
    elif line.startswith("ENTRY_ID="):
        remote_entry_id = line.split("=", 1)[1]
    elif line == "READY":
        break

print(f"Remote subprocess started  (pid {proc.pid})")
print(f"  PORT:     {remote_port}")
print(f"  SECRET:   {remote_secret}")
print(f"  ENTRY_ID: {remote_entry_id}")
```

## Step 2: Create the local node and store an entry

The main process gets its own policy, pool, and entry — completely independent from the subprocess:

```python
local_node = DefaultPolicy()
local_pool = DefaultPool()
laila.memory.extend(pool=local_pool, pool_nickname="main-store")

local_entry = laila.constant(
    data={"message": "Hello from the main process"},
    nickname="main-entry",
)
with laila.guarantee:
    laila.memorize(entries=local_entry, pool_nickname="main-store")

print("Local entry:", local_entry.global_id)
```

## Step 3: Peer the two processes

Open a TCP listener on the local side and call `add_tcpip_peer` with the remote's port and secret. The handshake is symmetric — both processes see each other as peers afterwards.

```python
local_tcp = DefaultTCPIPProtocol(
    host="127.0.0.1",
    port=0,
    peer_secret_key=remote_secret,
)

laila.active_policy = local_node
laila.communication.add_connection(local_tcp)
remote_id = laila.communication.add_tcpip_peer("127.0.0.1", remote_port, remote_secret)
time.sleep(0.3)

remote_proxy = laila.peers[remote_id]

print("Peered successfully.")
print(f"Local node:  {local_node.global_id}")
print(f"Remote peer: {remote_id}")
print(f"Peers:       {list(laila.peers.keys())}")
```

## Step 4: Main process requests the remote's entry

Setting `active_policy` to the remote proxy routes all `laila.*` calls to the subprocess over the TCP link. `remember` fetches the entry that only the remote process holds.

```python
laila.active_policy = remote_proxy
with laila.guarantee:
    result_b = laila.remember(entry_ids=remote_entry_id, pool_nickname="remote-store")

print("Main process retrieved from remote:")
print(" ", result_b.data[0])
```

## Step 5: Remote subprocess requests the main's entry

Peering is symmetric — the subprocess also sees the main process as a peer. Send the main entry's ID and pool nickname to the subprocess via stdin. It uses its own peer proxy to call `remember` on the main process and prints the result to stdout.

```python
proc.stdin.write(local_entry.global_id + "\n")
proc.stdin.write("main-store\n")
proc.stdin.flush()

for line in proc.stdout:
    line = line.strip()
    if line.startswith("REMOTE_RETRIEVED="):
        remote_got = line.split("=", 1)[1]
        break

print("Remote subprocess retrieved from main:")
print(" ", remote_got)
```

## Step 6: Clean up

Remove the local TCP connection and terminate the remote subprocess:

```python
laila.active_policy = local_node
laila.communication.remove_connection(local_tcp)
print("Local connection removed.")

proc.terminate()
proc.wait(timeout=5)
print("Remote subprocess terminated.")
```

## Summary

- The remote peer ran in a **separate subprocess** — the two sides shared nothing except the TCP link.
- `DefaultTCPIPProtocol` opens a TCP listener; `port=0` auto-selects a free port.
- `add_tcpip_peer` establishes the handshake; both sides must share the same `peer_secret_key`.
- After peering, `laila.peers[remote_global_id]` returns a **proxy** for the remote policy.
- Setting `laila.active_policy` to that proxy makes `memorize`, `remember`, and `forget` operate on the remote node.
- Peering is symmetric — both the main process and the subprocess successfully requested entries from each other.

Next: [Tutorial 10 — Accessing S3 Through a Remote Peer](10_peer_remote_s3.md), where you use peering to access cloud storage without local credentials.
