#!/usr/bin/env python3
"""Summarise a dataset folder so an agent can infer how datapoints map to each other.

Standard library only. Prints a human-readable report (and optionally a
JSON file) with:

- an extension histogram (count, total bytes) and the directory layout;
- every annotation file (json / jsonl / csv / tsv / txt) with its record
  count and field names / value types;
- join-key candidates: annotation fields whose values match asset file
  stems or relative paths, with the hit ratio and the cardinality
  (how many annotation rows per matched asset -> 1:1 or 1:N);
- segment candidates: numeric field pairs that look like time / frame
  ranges (start/end, t0/t1, ...) in files that also join to video or
  audio assets.

Usage:
    python scan_dataset.py <folder> [--json report.json] [--max-files 200000]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict

MEDIA = {
    "image": {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp", ".tif", ".tiff"},
    "video": {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"},
    "audio": {".wav", ".mp3", ".flac", ".ogg", ".m4a"},
    "array": {".npy", ".npz", ".pt", ".pth", ".safetensors", ".h5", ".hdf5"},
    "table": {".parquet", ".arrow", ".feather"},
}
ANNOTATION_EXT = {".json", ".jsonl", ".csv", ".tsv", ".txt"}
SEGMENT_PAIRS = [
    ("start", "end"),
    ("start_time", "end_time"),
    ("t0", "t1"),
    ("ts", "te"),
    ("begin", "end"),
    ("frame_start", "frame_end"),
    ("start_frame", "end_frame"),
    ("onset", "offset"),
]
SEGMENT_SINGLE = {"timestamp", "timestamps", "segment", "span", "clip", "time"}
SAMPLE_ROWS = 2000


def modality_of(ext: str) -> str | None:
    for name, exts in MEDIA.items():
        if ext in exts:
            return name
    return None


# ----------------------------------------------------------------------
# walking
# ----------------------------------------------------------------------
def walk(root: str, max_files: int):
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if fn.startswith("."):
                continue
            path = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(path)
            except OSError:
                continue
            files.append((os.path.relpath(path, root), size))
            if len(files) >= max_files:
                return files, True
    return files, False


# ----------------------------------------------------------------------
# annotation parsing
# ----------------------------------------------------------------------
def load_rows(path: str, ext: str, limit: int):
    """Return (rows, total_count_or_None). Rows are dicts (or {"value": ...})."""
    rows = []
    if ext == ".json":
        with open(path, encoding="utf-8", errors="replace") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            seq = data
        elif isinstance(data, dict):
            # {"annotations": [...]} style, or {id: {...}} style
            list_vals = [v for v in data.values() if isinstance(v, list)]
            if list_vals and all(isinstance(x, dict) for x in list_vals[0][:5]):
                seq = max(list_vals, key=len)
            elif data and all(isinstance(v, dict) for v in list(data.values())[:5]):
                seq = [{"__key__": k, **v} for k, v in data.items()]
            else:
                seq = [data]
        else:
            seq = [data]
        total = len(seq)
        rows = [r if isinstance(r, dict) else {"value": r} for r in seq[:limit]]
        return rows, total
    if ext == ".jsonl":
        total = 0
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                total += 1
                if len(rows) < limit:
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    rows.append(r if isinstance(r, dict) else {"value": r})
        return rows, total
    if ext in (".csv", ".tsv"):
        total = 0
        with open(path, encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t" if ext == ".tsv" else ",")
            for r in reader:
                total += 1
                if len(rows) < limit:
                    rows.append(dict(r))
        return rows, total
    if ext == ".txt":
        total = 0
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                total += 1
                if len(rows) < limit:
                    rows.append({"value": line.rstrip("\n")})
        return rows, total
    return rows, None


def field_types(rows):
    types = defaultdict(Counter)
    for r in rows:
        for k, v in r.items():
            types[k][type(v).__name__] += 1
    return {k: dict(c) for k, c in types.items()}


# ----------------------------------------------------------------------
# heuristics
# ----------------------------------------------------------------------
def join_candidates(rows, assets_by_stem, assets_by_relpath):
    """Fields whose values name asset files. Returns [(field, modality, hit_ratio, cardinality)]."""
    out = []
    if not rows:
        return out
    for field in rows[0].keys():
        values = [r.get(field) for r in rows if isinstance(r.get(field), (str, int))]
        if not values:
            continue
        hits = Counter()
        matched = Counter()
        for v in values:
            s = str(v)
            key = None
            if s in assets_by_relpath:
                key = ("relpath", s)
            else:
                stem = os.path.splitext(os.path.basename(s))[0]
                if stem in assets_by_stem:
                    key = ("stem", stem)
            if key is not None:
                mod = assets_by_relpath[s] if key[0] == "relpath" else assets_by_stem[key[1]]
                hits[mod] += 1
                matched[key[1]] += 1
        if not hits:
            continue
        mod, n = hits.most_common(1)[0]
        ratio = n / len(values)
        if ratio < 0.5:
            continue
        rows_per_asset = n / max(1, len(matched))
        out.append((field, mod, round(ratio, 3), round(rows_per_asset, 2)))
    return out


def segment_candidates(rows):
    if not rows:
        return []
    fields = set(rows[0].keys())
    low = {f.lower(): f for f in fields}
    found = []
    for a, b in SEGMENT_PAIRS:
        if a in low and b in low:
            fa, fb = low[a], low[b]
            numeric = sum(
                1
                for r in rows
                if isinstance(r.get(fa), (int, float)) and isinstance(r.get(fb), (int, float))
            )
            found.append({"fields": [fa, fb], "numeric_rows": numeric, "rows": len(rows)})
    for f in fields:
        fl = f.lower()
        if fl in SEGMENT_SINGLE or re.search(r"(^|_)(start|end)(_|$)", fl):
            if not any(f in c["fields"] for c in found):
                found.append({"fields": [f], "numeric_rows": None, "rows": len(rows)})
    return found


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def scan(root: str, max_files: int) -> dict:
    files, truncated = walk(root, max_files)
    ext_hist = defaultdict(lambda: [0, 0])
    dir_hist = Counter()
    assets_by_stem = {}
    assets_by_relpath = {}
    annotations = []
    for rel, size in files:
        ext = os.path.splitext(rel)[1].lower()
        ext_hist[ext][0] += 1
        ext_hist[ext][1] += size
        dir_hist[os.path.dirname(rel) or "."] += 1
        mod = modality_of(ext)
        if mod:
            assets_by_relpath[rel] = mod
            assets_by_stem.setdefault(os.path.splitext(os.path.basename(rel))[0], mod)
        elif ext in ANNOTATION_EXT:
            annotations.append((rel, ext, size))

    report = {
        "root": os.path.abspath(root),
        "files": len(files),
        "truncated": truncated,
        "extensions": {
            e: {"count": c, "bytes": b}
            for e, (c, b) in sorted(ext_hist.items(), key=lambda kv: -kv[1][1])
        },
        "directories": dict(dir_hist.most_common(40)),
        "assets": {mod: sum(1 for m in assets_by_relpath.values() if m == mod) for mod in MEDIA},
        "annotations": [],
    }

    for rel, ext, size in sorted(annotations, key=lambda t: -t[2])[:50]:
        path = os.path.join(root, rel)
        try:
            rows, total = load_rows(path, ext, SAMPLE_ROWS)
        except Exception as exc:
            report["annotations"].append({"file": rel, "error": repr(exc)})
            continue
        entry = {
            "file": rel,
            "bytes": size,
            "records": total,
            "fields": field_types(rows),
            "sample": rows[:2],
            "join_candidates": [
                {"field": f, "modality": m, "hit_ratio": r, "rows_per_asset": c}
                for f, m, r, c in join_candidates(rows, assets_by_stem, assets_by_relpath)
            ],
            "segment_candidates": segment_candidates(rows),
        }
        report["annotations"].append(entry)
    return report


def human(report: dict) -> str:
    lines = [
        f"# {report['root']}",
        f"files: {report['files']}" + (" (truncated)" if report["truncated"] else ""),
    ]
    lines.append("\n## extensions (by bytes)")
    for ext, d in report["extensions"].items():
        lines.append(f"  {ext or '<none>':10s} {d['count']:8d} files  {d['bytes'] / 1e6:10.1f} MB")
    lines.append("\n## assets by modality")
    for mod, n in report["assets"].items():
        if n:
            lines.append(f"  {mod:6s} {n}")
    lines.append("\n## directories (top)")
    for d, n in list(report["directories"].items())[:15]:
        lines.append(f"  {d:40s} {n}")
    lines.append("\n## annotation files")
    for a in report["annotations"]:
        if "error" in a:
            lines.append(f"  {a['file']}: ERROR {a['error']}")
            continue
        lines.append(f"  {a['file']}  records={a['records']}  bytes={a['bytes']}")
        lines.append(
            "    fields: " + ", ".join(f"{k}({'/'.join(v)})" for k, v in a["fields"].items())
        )
        for j in a["join_candidates"]:
            card = "1:1" if j["rows_per_asset"] <= 1.05 else f"1:{j['rows_per_asset']:g}"
            lines.append(
                f"    join: {j['field']} -> {j['modality']} files "
                f"(hit {j['hit_ratio']:.0%}, {card} rows per asset)"
            )
        for s in a["segment_candidates"]:
            lines.append(f"    segment: {' / '.join(s['fields'])}")
        if a["sample"]:
            lines.append("    sample: " + json.dumps(a["sample"][0], default=str)[:300])
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("folder")
    parser.add_argument(
        "--json", dest="json_path", default=None, help="also write the full report here"
    )
    parser.add_argument("--max-files", type=int, default=200_000)
    args = parser.parse_args(argv)
    if not os.path.isdir(args.folder):
        print(f"not a directory: {args.folder}", file=sys.stderr)
        return 2
    report = scan(args.folder, args.max_files)
    print(human(report))
    if args.json_path:
        with open(args.json_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"\nfull report written to {args.json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
