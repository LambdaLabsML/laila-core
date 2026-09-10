# Tutorial 24: MultiBuffer — A Ring of Records for Fast Producers

Every pool in LAILA is a *map*: entries keyed by `global_id`. A `MultiBuffer` is a *list*: a fixed number of slots addressed by integer index, with an independent **read head** and **write head** that walk the slots modulo the capacity. It is the container a microcontroller puts in front of a device that produces data faster than it can be persisted — the canonical example is a camera with a double or triple frame buffer.

You will:

- Create a `MultiBuffer`, write `Entry`, `Record`, and raw payloads, and read `Entry` objects back
- Watch the heads wrap around and the producer lap the consumer
- Use **mapped mode** over externally owned memory (a DMA-style list of `bytearray`s)
- Run a camera-style loop that `laila.memorize`s every frame and remembers one back
- See why a `MultiBuffer` is a `DataContainer` but *not* a pool

**Prerequisites:** `pip install laila-core`. No credentials or external services required.

```python
import laila
from laila.data import MultiBuffer
from laila.policy.central.memory.record.record import Record
```

## Step 1: A triple buffer

`capacity` defaults to 2 (a double buffer). Slots start empty (`None`) and both heads start at slot 0.

```python
buf = MultiBuffer(capacity=3)

print("gid:       ", buf.global_id)
print("len(buf):  ", len(buf))
print("heads:      read", buf.read_head, "| write", buf.write_head)
print("slots:     ", buf.slots)
```

## Step 2: Writing — the same value contract as pools

`write(value)` fills the slot under the write head and advances it, returning the slot index. Whatever you pass ends up stored as a `Record`, exactly as central memory does before a pool write:

- a `Record` is stored as-is,
- an `Entry` is wrapped in a fresh `Record`,
- a raw payload (`bytes`, `dict`, ...) is first lifted to a constant entry, then wrapped.

```python
i0 = buf.write(laila.constant(data={"frame": 0}))                 # Entry
i1 = buf.write(Record(entry=laila.constant(data={"frame": 1})))   # Record
i2 = buf.write(b"\x00\x01\x02")                                   # raw bytes

print("slots written:", (i0, i1, i2))
print("stored as:    ", [type(s).__name__ for s in buf.slots])
print("heads:         read", buf.read_head, "| write", buf.write_head, "(wrapped back to 0)")
```

## Step 3: Reading — always an `Entry`

`read()` returns the `Entry` at the read head and advances it. A `Record` yields its `entry`; raw slot contents are wrapped into a constant entry on the way out; an empty slot reads as `None` (the head still moves). Integer indexing `buf[i]` follows the same rules without touching the heads, and indices are taken modulo `capacity`.

```python
for _ in range(3):
    entry = buf.read()
    print(f"read -> {type(entry).__name__:5s} data={entry.data!r:22s} read_head now {buf.read_head}")

print("\nempty slot reads as:", MultiBuffer().read())
print("buf[4] is buf[1]:    ", buf[4].data == buf[1].data, "| buf[-1]:", buf[-1].data)

try:
    buf["x"]
except TypeError as exc:
    print("TypeError:", exc)
```

## Step 4: Wraparound and lapping

The heads are independent, so a producer may run ahead of the consumer by up to `capacity` slots. Beyond that it **laps** the consumer and overwrites the oldest unread slot — the trade-off a frame buffer deliberately makes (drop the oldest frame rather than stall the sensor).

```python
buf.empty()                                   # clear slots, rewind both heads
for payload in ("a", "b", "c", "d"):          # 4 writes into 3 slots
    buf.write(payload)

print("slot contents:", [buf[i].data for i in range(len(buf))], "<- 'a' was overwritten by 'd'")
print("write_head:", buf.write_head, "| read_head:", buf.read_head)
print("next read returns:", buf.read().data, "(the newest value, not 'a')")
```

## Step 5: Mapped mode — memory that something else fills

When the buffer stands in for memory owned by a DMA engine, a camera driver, or a shared `bytearray`, construct it with `mapped=True` and pass that memory as `slots`. The list is used **by reference** (never copied), `capacity` becomes `len(slots)`, and:

- `write()` with **no argument** only advances the write head — the slot was filled externally;
- `write(value)` deposits the raw value straight into the slot, bypassing the `Record` wrapping;
- `read()` is unchanged: it goes through `__getitem__`, which is where raw bytes become an `Entry`.

```python
dma = [bytearray(4) for _ in range(2)]          # "hardware" memory
frames = MultiBuffer(slots=dma, mapped=True)
print("capacity:", frames.capacity, "| mapped:", frames.mapped, "| same list object:", frames.slots is dma)

dma[0][:] = b"\x01\x02\x03\x04"                # the device fills slot 0 ...
frames.write()                                  # ... and we only bump the head
print("after external fill: write_head", frames.write_head, "| slot 0 raw:", frames.slots[0])

entry = frames.read()
print("read ->", type(entry).__name__, "with", type(entry.data).__name__, bytes(entry.data))

frames.write(b"\xff\xfe\xfd\xfc")            # deposit raw bytes directly into slot 1
print("slot 1 raw:", frames.slots[1], "| stored type:", type(frames.slots[1]).__name__)
print("read ->", bytes(frames.read().data))
```

## Step 6: The microcontroller loop — read a frame, memorize it

The buffer is the hand-off point; persistence is still `laila.memorize`. Each `read()` yields a fresh `Entry` with its own `global_id`, so five frames through a three-slot ring become five distinct entries in the policy's pool.

```python
camera = MultiBuffer(slots=[bytearray(8) for _ in range(3)], mapped=True)
stored = []

for k in range(5):
    camera.slots[camera.write_head][:] = bytes([k] * 8)   # sensor writes into the slot under the write head
    camera.write()                                         # publish it
    frame = camera.read()                                  # consumer takes the oldest unread frame ...
    laila.memorize(frame).wait()                           # ... and persists it through central memory
    stored.append(frame.global_id)

print("frames memorized:", len(stored), "| distinct gids:", len(set(stored)))
print("frame 3 remembered:", bytes(laila.remember(stored[3], persist=False).data))
```

## Step 7: A `DataContainer`, not a pool

`MultiBuffer` shares the `DataContainer` base (identity, atomic lock, value contract) with pools, but it is **not** a pool: it has no `global_id`-keyed lookup, so it cannot be a `dst_pool=` target and must not be registered with `laila.memory.extend`. Its job is to feed entries *into* `memorize`, not to store them.

Like every LAILA object it is CLI-capable: `capacity` and `mapped` are eligible configuration, while `slots` (live memory) is `CLIExempt`. `DefaultMultiBuffer` in `laila.macros.defaults` aliases the class.

```python
from laila.basics.definitions.cli_capable import build_environment
from laila.data.schema.base import _LAILA_IDENTIFIABLE_POOL
from laila.data.schema.data_container import _LAILA_IDENTIFIABLE_DATA_CONTAINER
from laila.macros.defaults import DefaultMultiBuffer

print("DataContainer:", isinstance(buf, _LAILA_IDENTIFIABLE_DATA_CONTAINER), "| pool:", isinstance(buf, _LAILA_IDENTIFIABLE_POOL))
print("DefaultMultiBuffer is MultiBuffer:", DefaultMultiBuffer is MultiBuffer)

try:
    laila.memorize(laila.constant(data=1), dst_pool=buf)
except TypeError as exc:
    print("dst_pool=buf ->", exc)

cli_view = {k: v for k, v in build_environment(buf)["policy"].items() if k in ("capacity", "mapped", "slots")}
print("CLI-eligible fields:", cli_view)
```

## Summary

- `MultiBuffer(capacity=n)` is a fixed ring of slots with independent read and write heads; `write()` returns the slot index, `read()` returns an `Entry` (or `None` for an empty slot), and both heads wrap modulo `capacity`.
- Writes accept a `Record`, an `Entry`, or a raw payload and store a `Record`, mirroring what central memory does before a pool write.
- A producer may run ahead by up to `capacity` slots; beyond that it laps the consumer and the oldest slot is overwritten.
- `mapped=True` wraps externally owned memory by reference; `write()` with no argument just advances the head, and `read()` turns the raw bytes into an `Entry`.
- Persist frames with `laila.memorize(entry)` as usual; the buffer is a `DataContainer` but not a pool, so it is never a `dst_pool` and never registered with the router.
- `capacity` and `mapped` are CLI-eligible; `slots` is exempt.
