"""Runtime argument loading from multiple file formats and the terminal."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional
import json
import sys
import xml.etree.ElementTree as ET


class ArgReader:
    """
    Load runtime args from simple key/value sources into the target mapping
    (defaults to ``laila.args``). All ``load`` / ``from_*`` methods mutate
    the target in place and return ``None``.
    """

    # Supported sources: .env, .json, .toml, .xml, or ``terminal`` (``key=value`` tokens).

    def __init__(self, target: Optional[Any] = None):
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
    def _flatten_one_level(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested dicts one level, joining keys with ``_``."""
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    out[f"{key}_{sub_key}"] = cls._coerce_scalar(sub_value)
            else:
                out[key] = cls._coerce_scalar(value)
        return out

    def _apply(self, payload: Dict[str, Any]) -> None:
        """Flatten and set each key/value pair on the target."""
        flat = self._flatten_one_level(payload)
        target = self._target_map()
        for key, value in flat.items():
            setattr(target, key, value)

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
        with open(path, "r", encoding="utf-8") as f:
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
        parsed: Dict[str, Any] = {}
        with open(path, "r", encoding="utf-8") as f:
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
        parsed: Dict[str, Any] = {}
        for child in root:
            children = list(child)
            if children:
                parsed[child.tag] = {gc.tag: (gc.text or "") for gc in children}
            else:
                parsed[child.tag] = child.text or ""
        self._apply(parsed)

    def from_terminal(self, args: Optional[Iterable[str]] = None) -> None:
        """Load arguments from ``key=value`` command-line tokens.

        Parameters
        ----------
        args : Iterable[str], optional
            Tokens to parse.  Defaults to ``sys.argv[1:]``.
        """
        tokens = list(sys.argv[1:] if args is None else args)
        parsed: Dict[str, Any] = {}
        for token in tokens:
            if "=" not in token:
                continue
            k, v = token.split("=", 1)
            key = k.strip()
            if not key:
                continue
            parsed[key] = self._coerce_scalar(v.strip())
        self._apply(parsed)

    def load(self, source: str | Path, *, terminal_args: Optional[Iterable[str]] = None) -> None:
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
