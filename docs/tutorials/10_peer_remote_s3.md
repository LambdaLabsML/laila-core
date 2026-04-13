# Tutorial 10: Accessing S3 Through a Remote Peer

Sometimes only one machine in a cluster has AWS credentials. Instead of distributing secrets, you can **peer** with that machine and use its S3 pool transparently — every `memorize`, `remember`, and `forget` call travels over the TCP link while the actual S3 traffic stays on the remote side.

In this tutorial **Node B** (a subprocess) holds the S3 credentials and pool. **Node A** (this notebook) has no S3 access at all. After peering, Node A morphs into Node B's policy via a proxy and stores / retrieves entries on S3 without ever touching AWS directly.

## Prerequisites

```bash
pip install "laila-core[s3]"
```

You will need an AWS S3 bucket and credentials. Store them in a `secrets.toml` — the notebook itself never reads these credentials; only the subprocess (Node B) uses them.

## Setup

```python
import sys
import subprocess
import textwrap
import time
import laila
from laila.macros.defaults import DefaultPolicy, DefaultTCPIPProtocol
```

## Step 1: Launch Node B with an S3 pool

The subprocess reads `secrets.toml`, creates an `S3Pool`, registers it under the nickname `"s3"`, and opens a TCP listener. It prints its connection info to stdout, then sleeps until terminated.

```python
REMOTE_SCRIPT = textwrap.dedent("""\
    import time, uuid, laila
    from laila.macros.defaults import DefaultTCPIPProtocol
    from laila.pool import S3Pool

    laila.read_args("./secrets.toml")

    s3_pool = S3Pool(
        bucket_name=laila.args.AWS_BUCKET_NAME,
        access_key_id=laila.args.AWS_ACCESS_KEY_ID,
        secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
        region_name=laila.args.AWS_REGION,
        nickname="s3",
    )
    laila.memory.extend(s3_pool, pool_nickname="s3")

    tcp = DefaultTCPIPProtocol(
        host="127.0.0.1",
        port=0,
        peer_secret_key=uuid.uuid4().hex,
    )
    laila.communication.add_connection(tcp)

    print(f"PORT={tcp.port}", flush=True)
    print(f"SECRET={tcp.peer_secret_key}", flush=True)
    print("READY", flush=True)

    while True:
        time.sleep(1)
""")

proc = subprocess.Popen(
    [sys.executable, "-c", REMOTE_SCRIPT],
    stdout=subprocess.PIPE,
    text=True,
)

remote_port = None
remote_secret = None
for line in proc.stdout:
    line = line.strip()
    if line.startswith("PORT="):
        remote_port = int(line.split("=", 1)[1])
    elif line.startswith("SECRET="):
        remote_secret = line.split("=", 1)[1]
    elif line == "READY":
        break

print(f"Remote subprocess started  (pid {proc.pid})")
print(f"  PORT:   {remote_port}")
print(f"  SECRET: {remote_secret}")
```

## Step 2: Create Node A and peer with Node B

Node A is a plain `DefaultPolicy` — no S3 pool, no AWS credentials. It only needs a TCP transport to reach Node B.

```python
local_node = DefaultPolicy()

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
print(f"Local node (A):  {local_node.global_id}")
print(f"Remote peer (B): {remote_id}")
```

## Step 3: Morph into Node B and memorize to S3

Setting `active_policy` to the remote proxy means every subsequent `laila.*` call is executed on Node B. The entries are created and then uploaded to S3 through Node B's pool — Node A never talks to AWS.

```python
laila.active_policy = remote_proxy

entry_config = laila.constant(
    data={"model": "resnet50", "epochs": 90, "lr": 0.001},
    nickname="training-config",
)
entry_metrics = laila.constant(
    data={"accuracy": 0.934, "loss": 0.187, "f1": 0.921},
    nickname="training-metrics",
)

with laila.guarantee:
    laila.memorize(entries=entry_config, pool_nickname="s3")
    laila.memorize(entries=entry_metrics, pool_nickname="s3")

config_gid = entry_config.global_id
metrics_gid = entry_metrics.global_id

print(f"Config entry:  {config_gid}")
print(f"Metrics entry: {metrics_gid}")
print("Both memorized to S3 via Node B.")
```

## Step 4: Remember from S3 through the peer

Delete the local entry objects, then recall them from S3 — still routed through Node B. The data round-trips through the subprocess's S3 pool and comes back over TCP.

```python
original_config = entry_config.data
original_metrics = entry_metrics.data
del entry_config, entry_metrics

with laila.guarantee:
    recalled_config = laila.remember(entry_ids=config_gid, pool_nickname="s3")
    recalled_metrics = laila.remember(entry_ids=metrics_gid, pool_nickname="s3")

print("Config recalled: ", recalled_config.data[0])
print("Metrics recalled:", recalled_metrics.data[0])

assert recalled_config.data[0] == original_config, "Config mismatch!"
assert recalled_metrics.data[0] == original_metrics, "Metrics mismatch!"
print("\nRound-trip verified — both entries match the originals.")
```

## Step 5: Clean up

Forget the entries from S3, switch back to the local policy, tear down the TCP connection, and terminate the subprocess:

```python
with laila.guarantee:
    laila.forget(config_gid, pool_nickname="s3")
    laila.forget(metrics_gid, pool_nickname="s3")
print("Entries removed from S3.")

laila.active_policy = local_node
laila.communication.remove_connection(local_tcp)
print("Local connection removed.")

proc.terminate()
proc.wait(timeout=5)
print("Remote subprocess terminated.")
```

## Summary

- Node B (subprocess) was the only process with AWS credentials and an `S3Pool`.
- Node A (this notebook) peered with B and set `active_policy` to B's proxy — "morphing" into the remote policy.
- `memorize`, `remember`, and `forget` with `pool_nickname="s3"` all routed through B's pool over TCP.
- Node A never imported `S3Pool` or touched AWS directly — all S3 traffic stayed on B's side.
- This pattern lets you centralise credentials on one machine while giving every peer transparent access to cloud storage.
