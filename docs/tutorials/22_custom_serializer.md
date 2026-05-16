# Tutorial 22: Writing a Custom Serializer

LAILA's default `pickle` serializer round-trips most Python objects, but you may want a more compact, more portable, or more version-stable format for your own types. The transformation registry lets you slot in a custom serializer behind the existing `memorize` / `remember` API.

## Prerequisites

```bash
pip install laila-core
```

## The custom type

A simple dataclass — a 2D polygon as a list of vertex tuples. Pickle would handle this, but a hand-written msgpack serializer is smaller and version-stable:

```python
from dataclasses import dataclass

@dataclass
class Polygon:
    vertices: list  # list[tuple[float, float]]
```

## A custom transformation

`_data_transformation` requires a `name`, plus `forward` (object → bytes) and `backward` (bytes → object). Subclasses with a non-empty `name` are auto-registered in `_data_transformation.REGISTRY`:

```python
import msgpack
from laila.entry.compdata.transformation.base import _data_transformation

class PolygonSerializer(_data_transformation):
    name: str = "polygon"

    def forward(self, data: Polygon) -> bytes:
        if not isinstance(data, Polygon):
            raise TypeError(f"PolygonSerializer expects Polygon, got {type(data)}")
        return msgpack.packb({"vertices": list(data.vertices)})

    def backward(self, data: bytes) -> Polygon:
        unpacked = msgpack.unpackb(data, raw=False)
        return Polygon(vertices=[tuple(v) for v in unpacked["vertices"]])
```

## A custom `ComputationalData` wrapper

`register_cdtype(Polygon)` declares that `Polygon` payloads should be wrapped by your new subclass. The wrapper class must override `__len__`, `shape`, `__copy__`, and `__deepcopy__` — the base raises `NotImplementedError` for those:

```python
from laila.entry.compdata.taxonomy.compdata import ComputationalData, register_cdtype

@register_cdtype(Polygon)
class CDPolygon(ComputationalData):
    def __len__(self) -> int:
        return len(self.data.vertices)

    @property
    def shape(self):
        return (len(self.data.vertices), 2)

    def __copy__(self):
        return CDPolygon(data=Polygon(vertices=list(self.data.vertices)))

    def __deepcopy__(self, memo):
        import copy
        return CDPolygon(data=Polygon(vertices=copy.deepcopy(self.data.vertices, memo)))
```

## Round-trip through any pool

```python
import laila
from laila.macros.defaults import DefaultPool

laila.memory.extend(DefaultPool(), pool_nickname="poly_store")

triangle = Polygon(vertices=[(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)])
entry = laila.constant(data=triangle, nickname="my_triangle")
laila.memorize(entry, pool_nickname="poly_store").wait()

recovered = laila.remember(nickname="my_triangle", pool_nickname="poly_store", persist=False).wait()
print(recovered.data)
# Polygon(vertices=[(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)])
```

## Notes on registry hygiene

- `_data_transformation` uses `__init_subclass__` to register at class-definition time. Importing your serializer module is enough — no explicit registration call.
- Names are wire-format-stable: renaming the `name` field is a breaking change for any pool that already holds entries encoded with the old name.
- Conflicts (two subclasses with the same `name`) are resolved last-write-wins by class import order.

## Summary

- A custom serializer is a `_data_transformation` subclass with `name`, `forward`, `backward`.
- `register_cdtype(MyType)` ties a `ComputationalData` subclass to a payload type so LAILA picks it up automatically.
- Once registered, the new type round-trips through every existing pool with no further changes.

Next: [Tutorial 23 — Building a Custom Pool Backend](23_custom_pool_backend.md).
