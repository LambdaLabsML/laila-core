# LAILA Tutorials

A progressive series of hands-on Jupyter notebooks that walk you through LAILA's core concepts, from creating your first entry to checkpointing an entire PyTorch model on S3.

Each tutorial builds on the one before it. If you are new to LAILA, start at the top.

## Tutorials

### 1. Entries and Identity — `01_entries_and_identity.ipynb`

Learn the most fundamental building block in LAILA: the **Entry**. This tutorial covers how to wrap arbitrary data (dicts, numpy arrays, torch tensors) using `laila.constant` and `laila.variable`, and explains the identity system — `global_id`, `uuid`, `scopes`, `evolution`, and `state`. You will also see how **nicknames** give entries deterministic, human-readable identities so the same name always maps to the same storage key.

**No credentials or external services required.**

### 2. Local Pools — `02_local_pools.ipynb`

Store and recall an entry across three local storage backends — **FilesystemPool**, **RedisPool**, and **HDF5Pool** — using a single for-loop. The point of this tutorial is that the `memorize` / `remember` / `forget` API is identical regardless of which backend you use. Only the pool constructor changes; everything else stays the same.

**Requires:** `pip install "laila-core[redis,hdf5]"`

### 3. S3 with NumPy and PyTorch Tensors — `03_s3_tensors.ipynb`

Move to cloud storage. Create an S3 pool, store a numpy matrix and a PyTorch tensor, delete the local references, then recall both objects purely by their `global_id`. This tutorial also introduces future inspection — checking status, reading exceptions, and using the future bank.

**Requires:** `pip install "laila-core[s3,torch]"` and a `secrets.toml` with AWS credentials.

### 4. Model Checkpoint and Reload — `04_model_checkpoint.ipynb`

The most advanced tutorial. Define a small CNN and an Adam optimizer, run a few training steps, then dump every weight tensor and the full optimizer state to S3 as individual entries. A single **manifest** entry ties them all together by mapping parameter names to `global_id` strings. After destroying all local state, you rebuild the entire model and optimizer from that one manifest — demonstrating how LAILA can serve as a checkpoint system backed by any storage pool.

**Requires:** `pip install "laila-core[s3,torch]"` and a `secrets.toml` with AWS credentials.

### 5. Sentiment Analysis Dataset with a Manifest — `05_sentiment_dataset_manifest.ipynb`

Build a small sentiment-analysis dataset as LAILA entries, organise them under a **Manifest**, push everything to S3, then remember and inspect the dataset from scratch using only the manifest nickname. Demonstrates how `Manifest` batches `memorize` / `remember` / `forget` and stores a nested blueprint that maps datapoint keys to `global_id` strings.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

### 6. Pool Proxies — Tiered Caching Across Backends — `06_pool_proxies.ipynb`

Wire a multi-tier cache chain with a single `<<` expression: `laila.alpha_pool << hdf5_pool << s3_pool`. When you read a key from the top-level pool, the request cascades down the chain until it finds the data, then caches a copy in every intermediate layer on the way back up. Writes, deletes, and existence checks stay local — only reads propagate. Demonstrates the `<<` / `>>` operators, `proxy_to` property, detaching and reattaching proxies at runtime, and the performance benefit of cache hits.

**Requires:** `pip install "laila-core[s3,hdf5]"` and a `secrets.toml` with AWS credentials.

### 7. Async Operations with Futures — `07_async_futures.ipynb`

Use Python's `async` / `await` with LAILA's futures to read numeric entries from S3, double each value, and write the results back without blocking the event loop. Covers three async patterns: awaiting a `GroupFuture` directly, awaiting individual futures via the future bank, and using `async with laila.guarantee_async:` to automatically track and await every future in scope.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

## Intermediate — `02_intermediate/`

### 8a. Saving the Environment to S3 — `08a_environment_to_s3.ipynb`

Set up LAILA with an S3 pool and an HDF5 pool, inspect `laila.environment`, wrap the full configuration dict as a **Manifest**, and upload it to S3. Demonstrates how `laila.environment` captures every CLI-eligible field in the live policy — pool settings, routing, command parameters — as a JSON-serialisable dict, and how storing it as a manifest makes the setup recoverable from anywhere.

**Requires:** `pip install "laila-core[s3,hdf5]"` and a `secrets.toml` with AWS credentials.

### 8b. Recovering a Policy from a Manifest — `08b_policy_from_manifest.ipynb`

Starting with nothing but S3 credentials, bootstrap a minimal policy, remember the environment manifest stored in **8a**, rebuild a fully-configured policy from the recovered dict, make it the active policy, and shut down the bootstrap policy. Shows how `laila.args = DotMap(env)` + `DefaultPolicy()` reconstructs the complete setup — including pools the bootstrap never registered.

**Requires:** `pip install "laila-core[s3]"`, a `secrets.toml` with AWS credentials, and a completed run of **8a**.

### 9. Peer-to-Peer Communication on Localhost — `09_peer_request_entry.ipynb`

Create two policies on `127.0.0.1`, peer them over TCP, store an entry on each node, and show that either side can request the other's data by switching `active_policy` to a peer proxy. Demonstrates `DefaultTCPIPProtocol`, `add_tcpip_peer`, `laila.peers`, and symmetric bidirectional access.

**No credentials or external services required.**

### 10. Accessing S3 Through a Remote Peer — `10_peer_remote_s3.ipynb`

Peer with a subprocess that holds AWS credentials and an S3 pool, morph into its policy via a proxy, and store / retrieve entries on S3 — all without local S3 access. Demonstrates how peering turns a remote node's storage into a transparent backend for the local process.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

## Getting started

```bash
pip install "laila-core[all]"
cd tutorials/
jupyter notebook
```

Open the first notebook and work through them in order.
