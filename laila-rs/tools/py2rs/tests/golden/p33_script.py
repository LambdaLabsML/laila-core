"""P33: a program -- top-level statements become `fn main()`."""

import laila

entries = [laila.constant(data=i) for i in range(3)]
for e in entries:
    print(e.global_id, e.data)
total = sum(e.data for e in entries)
print(f"total={total}")
