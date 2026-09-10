# Tutorial 10: Accessing S3 Through a Remote Peer

Sometimes only one machine in a cluster has AWS credentials. Instead of distributing secrets, you can **peer** with that machine and use its S3 pool transparently — every `memorize`, `remember`, and `forget` call travels over the peer link while the actual S3 traffic stays on the remote side.

In this tutorial **Node B** (a subprocess) holds the S3 credentials and pool. **Node A** (this notebook) has no S3 access at all. After peering, Node A targets B's `"s3"` pool with `dst_policy=` / `policy=` and stores / retrieves entries on S3 without ever touching AWS directly.

You will:

- Launch a remote subprocess (Node B) that registers an `S3Pool`
- Create a local policy (Node A) with no S3 access and peer with B via `laila.add_peer`
- Memorize entries to S3 through B with `dst_policy=B, dst_pool="s3"`
- Delete local references and remember the entries back from S3
- Compare with **morph mode** (`laila.active_policy = proxy`)
- Clean up: forget the entries from S3 and terminate the subprocess

**Prerequisites:** `pip install "laila-core[s3]"` and a `secrets.toml` with your AWS credentials.

```python
import subprocess
import sys
import textwrap
import time

import laila
from laila.macros.defaults import DefaultPolicy, DefaultWebSocketProtocol
```

## Load credentials

Create a `secrets.toml` in the same directory:
```toml
AWS_BUCKET_NAME = "your-bucket"
AWS_ACCESS_KEY_ID = "AKIA..."
AWS_SECRET_ACCESS_KEY = "wJa..."
AWS_REGION = "us-east-1"
```

The notebook itself never reads these credentials — only the subprocess (Node B) uses them. The file just needs to be in the working directory so the subprocess can find it.

## Step 1: Launch Node B with an S3 pool

The subprocess reads `secrets.toml`, activates its own policy, creates an `S3Pool`, registers it under the nickname `"s3"`, and opens a WebSocket listener on a free port. It prints its connection info to stdout, then sleeps until terminated.

```python
REMOTE_SCRIPT = textwrap.dedent("""\
    import time, uuid, laila
    from laila.macros.defaults import DefaultPolicy, DefaultWebSocketProtocol
    from laila.data import S3Pool

    laila.read_args("./secrets.toml")

    node_b = DefaultPolicy()
    laila.activate_policy(node_b)

    s3_pool = S3Pool(
        bucket_name=laila.args.AWS_BUCKET_NAME,
        access_key_id=laila.args.AWS_ACCESS_KEY_ID,
        secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
        region_name=laila.args.AWS_REGION,
        nickname="s3",
    )
    laila.memory.extend(s3_pool, pool_nickname="s3")

    ws = DefaultWebSocketProtocol(host="127.0.0.1", port=0, peer_secret_key=uuid.uuid4().hex)
    laila.communication.add_connection(ws)

    print(f"PORT={ws.bound_port}", flush=True)
    print(f"SECRET={ws.peer_secret_key}", flush=True)
    print(f"POLICY_ID={node_b.global_id}", flush=True)
    print("READY", flush=True)

    while True:
        time.sleep(1)
""")

proc = subprocess.Popen(
    [sys.executable, "-c", REMOTE_SCRIPT],
    stdout=subprocess.PIPE,
    text=True,
)

info = {}
for line in proc.stdout:
    line = line.strip()
    if line == "READY":
        break
    key, _, value = line.partition("=")
    info[key] = value

remote_port = int(info["PORT"])
remote_secret = info["SECRET"]

print(f"Node B started  (pid {proc.pid})")
print(f"  PORT:      {remote_port}")
print(f"  SECRET:    {remote_secret}")
print(f"  POLICY_ID: {info['POLICY_ID']}")
```

## Step 2: Create Node A and peer with Node B

Node A is a plain `DefaultPolicy` — no S3 pool, no AWS credentials. It only needs a WebSocket transport to reach Node B. `laila.add_peer("ws://...", secret)` performs the handshake and returns B's `global_id`.

```python
node_a = DefaultPolicy()
laila.activate_policy(node_a)

local_ws = DefaultWebSocketProtocol(host="127.0.0.1", port=0, peer_secret_key=remote_secret)
laila.communication.add_connection(local_ws)

node_b_id = laila.add_peer(f"ws://127.0.0.1:{remote_port}", remote_secret)
time.sleep(0.3)

print("Peered successfully.")
print(f"Local node (A):  {node_a.global_id}")
print(f"Remote peer (B): {node_b_id}")
print(f"Pools on A:      {list(laila.memory.pool_router.pools_nicknames)}")
```

## Step 3: Memorize to S3 through Node B

`laila.memorize(entry, dst_policy=node_b_id, dst_pool="s3")` ships the entry to B, which writes it into its `"s3"` pool. Node A's active policy is untouched and Node A never talks to AWS. The returned future is a normal local future; `.data` is the stored gid.

```python
entry_config = laila.constant(
    data={"model": "resnet50", "epochs": 90, "lr": 0.001},
    nickname="training-config",
)
entry_metrics = laila.constant(
    data={"accuracy": 0.934, "loss": 0.187, "f1": 0.921},
    nickname="training-metrics",
)

with laila.guarantee:
    laila.memorize(entry_config, dst_policy=node_b_id, dst_pool="s3")
    laila.memorize(entry_metrics, dst_policy=node_b_id, dst_pool="s3")

config_gid = entry_config.global_id
metrics_gid = entry_metrics.global_id

print(f"Config entry:  {config_gid}")
print(f"Metrics entry: {metrics_gid}")
print("Both memorized to S3 via Node B; active policy still A:",
      laila.active_policy.global_id == node_a.global_id)
```

## Step 4: Remember from S3 through the peer

Delete the local entry objects, then recall them from S3 — still routed through Node B. B reads from its S3 pool, serializes the entry, and A rebuilds it locally. `persist=False` keeps B from also caching a copy in its alpha pool.

```python
original_config = entry_config.data
original_metrics = entry_metrics.data
del entry_config, entry_metrics

recalled_config = laila.remember(config_gid, dst_policy=node_b_id, dst_pool="s3", persist=False)
recalled_metrics = laila.remember(metrics_gid, dst_policy=node_b_id, dst_pool="s3", persist=False)

print("Config recalled: ", recalled_config.data)
print("Metrics recalled:", recalled_metrics.data)

assert recalled_config.data == original_config, "Config mismatch!"
assert recalled_metrics.data == original_metrics, "Metrics mismatch!"
print("\nRound-trip verified — both entries match the originals.")
```

## Step 5: The same thing in morph mode

Assigning B's proxy to `laila.active_policy` makes *every* `laila.*` call run on B, so you can drop `dst_policy=` and address B's pools as if they were local. Futures come back as `RemoteFuture`s. This is handy for a burst of calls against one peer, but the `dst_policy=` form above is preferred because the active policy never changes underneath you.

```python
laila.active_policy = laila.peers[node_b_id]

morphed = laila.remember(config_gid, dst_pool="s3", persist=False)
print("Future type:", type(morphed).__name__)
print("Config via morph:", morphed.data)

laila.active_policy = node_a
print("Back on A:", laila.active_policy.global_id == node_a.global_id)
```

## Step 6: Clean up

Forget the entries from S3 (on B, with `policy=` / `pool=`), tear down the local transport, and terminate the subprocess.

```python
with laila.guarantee:
    laila.forget(config_gid, policy=node_b_id, pool="s3")
    laila.forget(metrics_gid, policy=node_b_id, pool="s3")
print("Entries removed from S3.")

laila.communication.remove_connection(local_ws)
print("Local connection removed.")

proc.terminate()
proc.wait(timeout=5)
print("Remote subprocess terminated.")
```

## Summary

- Node B (subprocess) was the only process with AWS credentials and an `S3Pool`.
- Node A peered with B via `laila.add_peer("ws://...", secret)`.
- `memorize` / `remember` with `dst_policy=node_b_id, dst_pool="s3"` and `forget` with `policy=node_b_id, pool="s3"` all executed on B's pool while A's active policy stayed local.
- Morph mode (`laila.active_policy = laila.peers[gid]`) achieves the same with implicit routing and `RemoteFuture`s.
- Node A never imported `S3Pool` or touched AWS directly — all S3 traffic stayed on B's side. This pattern lets you centralise credentials on one machine while giving every peer transparent access to cloud storage.

Next: [Tutorial 11 — Lazy Entries with Constitutions](11_constitutions_and_build.md).
