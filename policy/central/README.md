# Central

Central includes the following 4 components:

- [`command`](./command/README.md): the workhorse of LAILA, taking care of compute or IO-heavy work.
- [`communication`](./communication/README.md): the backbone of connections to other policies.
- [`control`](./control/README.md): compute graph operations and dependency manifest.
- [`memory`](./memory/README.md): the main memory of LAILA.

`communication` and `control` are not part of the beta 1.0 release.
