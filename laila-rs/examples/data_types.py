"""Every Python data type laila handles, round-tripped through a pool.

Script form of ``examples/simple/data_types.ipynb``: wrap one value of each
type as a constant entry, memorize the lot into the active policy's alpha
pool inside a ``laila.guarantee`` scope, remember each one back and check
that both the value and its type survived. The torch tensor of the
notebook is left out (the Rust twin has no torch runtime).

    python laila-rs/examples/data_types.py
    cargo run -p laila-core --example data_types
"""

import laila
import numpy as np
from laila.macros.defaults import DefaultPolicy

# Every laila verb routes through the active policy; create one explicitly
# so the example does not depend on laila.json / LAILA_* env.
policy = DefaultPolicy()
laila.activate_policy(policy)

# One sample value per data type.
samples = {
    "int": 42,
    "float": 3.14159,
    "str": "hello laila",
    "bool": True,
    "none": None,
    "list": [1, 2, 3, "four"],
    "tuple": ("alpha", "beta", "gamma"),
    "set": {"x", "y", "z"},
    "dict": {"name": "laila", "version": 1, "active": True},
    "numpy": np.array([[1.0, 2.0], [3.0, 4.0]]),
}

# Wrap each value as a LAILA constant to assign global ids.
wrapped = {}
for name, data in samples.items():
    wrapped[name] = laila.constant(data=data, nickname=f"sample-{name}")
    print(f"{name:>12} -> {wrapped[name].global_id}")

# ``with laila.guarantee:`` waits for every future submitted inside it.
print("\nmemorizing into the alpha pool ...")
with laila.guarantee:
    laila.memorize(entries=list(wrapped.values()))

# Remember each entry back and compare value and type.
print("\nremembering ...")
ok = 0
for name, entry in wrapped.items():
    remembered = laila.remember(entry.global_id).wait()
    data = remembered.data
    if name == "numpy":
        same = bool(np.array_equal(data, samples[name]))
    else:
        same = data == samples[name] and type(data) is type(samples[name])
    print(f"{name:>12} -> {type(data).__name__:<8} {'ok' if same else 'MISMATCH'}")
    ok += int(same)

print(f"\n{ok}/{len(samples)} round trips preserved value and type")
laila.terminate()
