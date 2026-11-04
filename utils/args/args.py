"""Runtime argument loading from multiple file formats and the terminal.

Defines :class:`ArgReader`, the loader behind :data:`laila.args`. The
class accepts arguments from any of the following formats and merges
them into a single attribute-accessible mapping (defaulting to
``laila.args``, which is an :class:`AtomicDotMap`):

- ``.json`` -- JSON object at the top level (:meth:`from_json`).
- ``.toml`` -- TOML table at the top level (:meth:`from_toml`).
- ``.env`` -- ``KEY=VALUE`` lines, ``#`` comments allowed
  (:meth:`from_env`). A file literally named ``.env`` works too.
- ``.xml`` -- one level of nesting allowed; siblings become nested
  dicts (:meth:`from_xml`).
- ``terminal`` -- ``key=value``, ``--key=value``, ``--key value`` and
  bare ``--flag`` (-> ``True``) tokens parsed from ``sys.argv``
  (:meth:`from_terminal`).
- ``load(source)`` -- auto-detects format from the file suffix.

Values are coerced from strings to the most specific Python scalar
they fit (bool, None, int, float, JSON-shaped collection, quoted
string, fall-back raw string) by :meth:`_coerce_scalar`. Nested
dicts in JSON / TOML files are applied twice: flattened *one level*
with underscore-joined keys (``s3_bucket_name``) for the flat,
shell-style surface, **and** deep-merged into the nested tree
(``laila.args.s3.bucket_name``) so a config file can drive the
CLI-capable defaults that live under ``laila.args.policy...`` /
``laila.args.logger``. Merging never replaces an existing nested
mapping wholesale: keys set earlier (by code or another file) survive
unless the new file sets the same leaf.
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def _looks_numeric(token: str) -> bool:
    """``True`` for tokens such as ``-3`` / ``-0.5`` (values, not flags)."""
    try:
        float(token)
    except ValueError:
        return False
    return True


class ArgReader:
    """Loader of runtime args into an attribute-accessible target mapping.

    All ``load`` / ``from_*`` methods mutate the target in place and
    return ``None``. The target defaults to ``laila.args`` (an
    :class:`AtomicDotMap`) when not specified, so the typical usage
    pattern is just::

        reader = ArgReader()
        reader.load("config.json")
        reader.load("terminal")

    A custom target lets test suites or library callers run with a
    private DotMap-like object instead.

    Notes
    -----
    The reader is intentionally tolerant: malformed lines in
    ``.env``-style files are skipped, JSON-coerced collections that
    fail to parse fall back to the original string, etc. The aim is
    to never lose arguments to a strict parse error -- user-friendly
    "best-effort" semantics.
    """

    # Supported sources: .env, .json, .toml, .xml, or ``terminal`` (``key=value`` tokens).

    def __init__(self, target: Any | None = None):
        """Initialise the reader.

        Parameters
        ----------
        target : Any, optional
            Object to set attributes on.  Defaults to ``laila.args``.
        """
        self._target = target

    def _target_map(self) -> Any:
        """Return the target mapping, falling back to ``laila.args``."""
        if self._target is not None:
            return self._target
        import laila  # lazy import to avoid circular import at module load

        return laila.args

    @staticmethod
    def _coerce_scalar(value: Any) -> Any:
        """Coerce a string value to its most specific Python scalar type."""
        if not isinstance(value, str):
            return value

        stripped = value.strip()
        lowered = stripped.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        if lowered in {"none", "null"}:
            return None

        try:
            return int(stripped)
        except ValueError:
            pass

        try:
            return float(stripped)
        except ValueError:
            pass

        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                return json.loads(stripped)
            except Exception:
                return value

        if (
            (stripped.startswith('"') and stripped.endswith('"'))
            or (stripped.startswith("'") and stripped.endswith("'"))
        ) and len(stripped) >= 2:
            return stripped[1:-1]

        return value

    @classmethod
    def _flatten_one_level(cls, payload: dict[str, Any]) -> dict[str, Any]:
        """Flatten nested dicts one level, joining keys with ``_``."""
        out: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    out[f"{key}_{sub_key}"] = cls._coerce_scalar(sub_value)
            else:
                out[key] = cls._coerce_scalar(value)
        return out

    @classmethod
    def _coerce_tree(cls, value: Any) -> Any:
        """Coerce scalars recursively inside nested dicts / lists."""
        if isinstance(value, dict):
            return {k: cls._coerce_tree(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._coerce_tree(v) for v in value]
        return cls._coerce_scalar(value)

    @staticmethod
    def _deep_merge(target: Any, key: str, value: dict[str, Any]) -> None:
        """Merge the nested dict *value* into ``target[key]`` leaf by leaf.

        An existing mapping under *key* (a ``DotMap`` or ``dict``) is
        updated in place; missing branches are created; non-mapping
        values at either side are replaced by the incoming leaf.
        """
        from dotmap import DotMap

        def _lookup(container: Any, k: str) -> Any:
            # ``AtomicDotMap`` has no ``get``/``__getitem__`` and answers
            # every unknown attribute (including ``get``) with ``None``,
            # so resolve through the *type* before trusting a callable.
            getter = getattr(type(container), "get", None)
            if callable(getter):
                try:
                    return container.get(k)
                except Exception:
                    return None
            try:
                return getattr(container, k)
            except AttributeError:
                return None

        def _store(container: Any, k: str, v: Any) -> None:
            if isinstance(container, DotMap):
                setattr(container, k, DotMap(v) if isinstance(v, dict) else v)
            elif isinstance(container, dict):
                container[k] = v
            else:
                setattr(container, k, v)

        existing = _lookup(target, key)
        mapping_like = isinstance(existing, (dict, DotMap)) or (
            existing is not None and callable(getattr(type(existing), "__setitem__", None))
        )
        if not mapping_like:
            # ``DotMap(dict)`` converts nested dicts recursively so the
            # branch stays attribute-accessible all the way down.
            _store(target, key, value)
            return
        for sub_key, sub_value in value.items():
            if isinstance(sub_value, dict):
                ArgReader._deep_merge(existing, sub_key, sub_value)
            else:
                existing[sub_key] = sub_value

    def _apply(self, payload: dict[str, Any]) -> None:
        """Apply *payload* to the target: flat ``a_b`` keys plus the nested tree.

        Top-level scalars are set as-is. A dict-valued top-level key is
        set both ways: its first nesting level flattened with ``_``
        (``policy_central``) **and** deep-merged under the original key
        (``args.policy.central...``), which is where CLI-capable classes
        resolve their defaults from. The ``environment`` key is applied
        last so an environment load (which calls ``terminate`` and
        rebuilds the policy graph) sees every other key already in place.
        """
        flat = self._flatten_one_level(payload)
        target = self._target_map()
        for key, value in flat.items():
            setattr(target, key, value)
        nested = [(k, v) for k, v in payload.items() if isinstance(v, dict)]
        nested.sort(key=lambda kv: kv[0] == "environment")
        for key, value in nested:
            self._deep_merge(target, key, self._coerce_tree(value))

    def clear(self) -> None:
        """Remove all keys from the target mapping."""
        target = self._target_map()
        if hasattr(target, "keys"):
            for key in list(target.keys()):
                try:
                    delattr(target, key)
                except Exception:
                    pass

    def from_json(self, path: str | Path) -> None:
        """Load arguments from a JSON file.

        Parameters
        ----------
        path : str or Path
            Path to the ``.json`` file.
        """
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("JSON args file must be a key/value object.")
        self._apply(data)

    def from_toml(self, path: str | Path) -> None:
        """Load arguments from a TOML file.

        Parameters
        ----------
        path : str or Path
            Path to the ``.toml`` file.
        """
        try:
            import tomllib  # Python 3.11+
        except ImportError as e:
            raise ImportError("tomllib is required for TOML parsing.") from e

        with open(path, "rb") as f:
            data = tomllib.load(f)
        if not isinstance(data, dict):
            raise ValueError("TOML args file must be a key/value table.")
        self._apply(data)

    def from_env(self, path: str | Path) -> None:
        """Load arguments from a ``.env``-style file.

        Parameters
        ----------
        path : str or Path
            Path to the ``.env`` file.
        """
        parsed: dict[str, Any] = {}
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                parsed[k.strip()] = self._coerce_scalar(v.strip())
        self._apply(parsed)

    def from_xml(self, path: str | Path) -> None:
        """Load arguments from an XML file.

        Parameters
        ----------
        path : str or Path
            Path to the ``.xml`` file.
        """
        tree = ET.parse(path)
        root = tree.getroot()
        parsed: dict[str, Any] = {}
        for child in root:
            children = list(child)
            if children:
                parsed[child.tag] = {gc.tag: (gc.text or "") for gc in children}
            else:
                parsed[child.tag] = child.text or ""
        self._apply(parsed)

    def from_terminal(self, args: Iterable[str] | None = None) -> None:
        """Load arguments from command-line tokens.

        Accepted forms (dashes are stripped from the key):

        - ``key=value`` / ``--key=value`` / ``-key=value``
        - ``--key value`` (the next token, unless it is itself a flag)
        - ``--flag`` (no value) -> ``True``

        Bare tokens without ``=`` or a leading dash are ignored.

        Parameters
        ----------
        args : Iterable[str], optional
            Tokens to parse.  Defaults to ``sys.argv[1:]``.
        """
        tokens = list(sys.argv[1:] if args is None else args)
        parsed: dict[str, Any] = {}
        i = 0
        while i < len(tokens):
            token = tokens[i]
            i += 1
            dashed = token.startswith("-") and not _looks_numeric(token)
            if "=" in token:
                k, v = token.split("=", 1)
                key = k.strip().lstrip("-") if dashed else k.strip()
                if not key:
                    continue
                parsed[key] = self._coerce_scalar(v.strip())
                continue
            if not dashed:
                continue
            key = token.strip().lstrip("-")
            if not key:
                continue
            if i < len(tokens):
                nxt = tokens[i]
                if "=" not in nxt and not (nxt.startswith("-") and not _looks_numeric(nxt)):
                    parsed[key] = self._coerce_scalar(nxt.strip())
                    i += 1
                    continue
            parsed[key] = True
        self._apply(parsed)

    def load(self, source: str | Path, *, terminal_args: Iterable[str] | None = None) -> None:
        """Auto-detect format and load arguments.

        Parameters
        ----------
        source : str or Path
            File path (suffix selects format) or the literal ``"terminal"``.
        terminal_args : Iterable[str], optional
            Passed to :meth:`from_terminal` when *source* is ``"terminal"``.

        Raises
        ------
        ValueError
            If the file suffix is not supported.
        """
        p = Path(source)
        suffix = p.suffix.lower()
        if not suffix and p.name.lower() == ".env":
            suffix = ".env"  # ``Path(".env").suffix`` is ``""``
        if suffix == ".json":
            self.from_json(p)
            return
        if suffix == ".toml":
            self.from_toml(p)
            return
        if suffix == ".env":
            self.from_env(p)
            return
        if suffix == ".xml":
            self.from_xml(p)
            return
        if str(source).lower() == "terminal":
            self.from_terminal(terminal_args)
            return
        raise ValueError(f"Unsupported args source: {source}")
