#!/usr/bin/env python3
"""
ApexPilot -> Splunk log exporter.

Converts openpilot/sunnypilot drive logs (rlog / qlog capnp segments) into
Splunk-ready events. Two output modes:

  * file  (default, fully OFFLINE): writes newline-delimited JSON (NDJSON) that
          Splunk ingests with a `sourcetype=_json` monitor input or a one-time
          `oneshot` upload. Nothing leaves the device until *you* move the file.
  * hec   (optional, ONLINE): POSTs batched events to a Splunk HTTP Event
          Collector. Only used if you explicitly pass --mode hec/both.

Design goals:
  - No network required in the default path (respects the offline build).
  - No hard dependency on comma's networked route tools: it reads the capnp
    segments directly with `cereal`. (LogReader is supported as a fallback.)
  - Safe field selection + size guards so you don't dump raw camera/model
    tensors into Splunk.

Usage examples
--------------
  # Offline: dump every segment under the default realdata dir to NDJSON
  python3 splunk_exporter.py --input /data/media/0/realdata --out ./export

  # Offline: one route, only the message types listed in the config
  python3 splunk_exporter.py --input /data/media/0/realdata/<route> \
      --config splunk_config.json --out ./export

  # Online: stream to Splunk HEC (token/url can also live in splunk_config.json)
  python3 splunk_exporter.py --input /data/media/0/realdata \
      --mode hec --hec-url https://splunk:8088 --hec-token XXXX

Exit codes: 0 ok, 2 bad args, 3 nothing found, 4 HEC delivery failure.
"""
from __future__ import annotations

import argparse
import base64
import bz2
import datetime as _dt
import glob
import gzip
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
DEFAULT_REALDATA = "/data/media/0/realdata"

# Message types that are useful in Splunk for test/telemetry review. Keep this
# list tight: openpilot emits ~100 msg types at up to 100 Hz, and some carry
# large tensors (modelV2, camera*). An empty list means "export everything
# except the heavy types in HEAVY_TYPES".
DEFAULT_WHITELIST: List[str] = [
    "carState",
    "carControl",
    "controlsState",
    "selfdriveState",
    "liveParameters",
    "deviceState",
    "driverMonitoringState",
    "driverStateV2",
    "onroadEvents",
    "gpsLocationExternal",
    "gpsLocation",
    "liveLocationKalman",
    "peripheralState",
    "pandaStates",
    "managerState",
    "errorLogMessage",
]

# Never export these unless the user forces it: they contain big arrays.
HEAVY_TYPES = {
    "modelV2", "cameraOdometry", "roadCameraState", "driverCameraState",
    "wideRoadCameraState", "thumbnail", "navModel", "drivingModelData",
    "uploaderState", "qRoadEncodeData", "roadEncodeData", "driverEncodeData",
    "wideRoadEncodeData", "livestreamRoadEncodeData",
}

# Truncate any list field longer than this (keeps Splunk events sane).
MAX_LIST_LEN = 64
# Skip any single event whose JSON exceeds this many bytes.
MAX_EVENT_BYTES = 64 * 1024
# HEC batch size (events per POST).
HEC_BATCH = 200


# ----------------------------------------------------------------------------
# capnp decoding
# ----------------------------------------------------------------------------
def _load_cereal():
    """Import cereal.log. Requires openpilot root on PYTHONPATH."""
    try:
        from cereal import log  # type: ignore
        return log
    except Exception as e:  # pragma: no cover - env dependent
        sys.stderr.write(
            "ERROR: could not import `cereal`. Run this from the ApexPilot "
            "repo root, or set PYTHONPATH to it, e.g.\n"
            "  PYTHONPATH=/data/openpilot python3 splunk_exporter.py ...\n"
            f"(import error: {e})\n"
        )
        raise


def _open_segment(path: str):
    """Open an rlog/qlog segment file, transparently decompressing."""
    if path.endswith(".bz2"):
        return bz2.open(path, "rb")
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    if path.endswith(".zst") or path.endswith(".zstd"):
        try:
            import zstandard  # type: ignore
        except Exception:
            raise RuntimeError(
                f"{path} is zstd-compressed but the `zstandard` module is not "
                "installed. `pip install zstandard --break-system-packages`."
            )
        dctx = zstandard.ZstdDecompressor()
        return dctx.stream_reader(open(path, "rb"))
    return open(path, "rb")


def iter_messages(path: str, log) -> Iterator[Any]:
    """Yield capnp Event readers from a single segment file."""
    f = _open_segment(path)
    try:
        # read_multiple handles the framed capnp stream used by rlog/qlog.
        for event in log.Event.read_multiple(f):
            yield event
    finally:
        try:
            f.close()
        except Exception:
            pass


# ----------------------------------------------------------------------------
# capnp -> JSON
# ----------------------------------------------------------------------------
def _coerce(value: Any) -> Any:
    """Recursively convert a capnp value into JSON-serializable Python."""
    # bytes -> base64 (so Splunk gets a clean string, not a decode error)
    if isinstance(value, (bytes, bytearray)):
        if len(value) > MAX_LIST_LEN:
            return {"_b64_truncated": base64.b64encode(bytes(value[:MAX_LIST_LEN])).decode(),
                    "_orig_len": len(value)}
        return base64.b64encode(bytes(value)).decode()

    # capnp enums stringify cleanly
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)) or value is None:
        # guard against inf/nan which are not valid JSON
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            return str(value)
        return value

    # capnp struct reader -> dict
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _coerce(value.to_dict())
        except Exception:
            pass

    # dict
    if isinstance(value, dict):
        return {str(k): _coerce(v) for k, v in value.items()}

    # list / sequence
    if isinstance(value, (list, tuple)):
        seq = list(value)
        if len(seq) > MAX_LIST_LEN:
            return {"_list_truncated": [_coerce(v) for v in seq[:MAX_LIST_LEN]],
                    "_orig_len": len(seq)}
        return [_coerce(v) for v in seq]

    # capnp list reader or other iterable
    try:
        seq = list(value)
        if len(seq) > MAX_LIST_LEN:
            return {"_list_truncated": [_coerce(v) for v in seq[:MAX_LIST_LEN]],
                    "_orig_len": len(seq)}
        return [_coerce(v) for v in seq]
    except TypeError:
        return str(value)


def event_to_record(event: Any, route: str, segment: str) -> Optional[Dict[str, Any]]:
    """Turn one capnp Event into a flat Splunk event dict, or None to skip."""
    which = event.which()
    try:
        sub = getattr(event, which)
    except Exception:
        sub = None

    body = _coerce(sub)
    # logMonoTime is nanoseconds since boot; valid flag lives on the event.
    mono = int(getattr(event, "logMonoTime", 0) or 0)
    valid = bool(getattr(event, "valid", True))

    record = {
        "which": which,
        "logMonoTime": mono,
        "valid": valid,
        "route": route,
        "segment": segment,
        "data": body,
    }
    return record


# ----------------------------------------------------------------------------
# Splunk HEC sink
# ----------------------------------------------------------------------------
class HECSink:
    def __init__(self, url: str, token: str, index: Optional[str],
                 sourcetype: str, source: str, verify_tls: bool = True):
        self.endpoint = url.rstrip("/") + "/services/collector/event"
        self.token = token
        self.index = index
        self.sourcetype = sourcetype
        self.source = source
        self.verify_tls = verify_tls
        self._buf: List[str] = []
        self._sent = 0
        # Prefer requests; fall back to urllib so there's no hard dependency.
        try:
            import requests  # type: ignore
            self._requests = requests
        except Exception:
            self._requests = None

    def _wrap(self, rec: Dict[str, Any]) -> str:
        envelope = {
            "time": rec["logMonoTime"] / 1e9 if rec.get("logMonoTime") else time.time(),
            "event": rec,
            "sourcetype": self.sourcetype,
            "source": self.source,
        }
        if self.index:
            envelope["index"] = self.index
        return json.dumps(envelope, separators=(",", ":"))

    def add(self, rec: Dict[str, Any]) -> None:
        self._buf.append(self._wrap(rec))
        if len(self._buf) >= HEC_BATCH:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        payload = "\n".join(self._buf).encode("utf-8")
        headers = {"Authorization": f"Splunk {self.token}",
                   "Content-Type": "application/json"}
        last_err = None
        for attempt in range(3):
            try:
                if self._requests is not None:
                    r = self._requests.post(self.endpoint, data=payload,
                                            headers=headers, timeout=15,
                                            verify=self.verify_tls)
                    if r.status_code // 100 == 2:
                        break
                    last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                else:
                    import ssl
                    import urllib.request
                    ctx = None
                    if not self.verify_tls:
                        ctx = ssl.create_default_context()
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                    req = urllib.request.Request(self.endpoint, data=payload,
                                                 headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                        if resp.status // 100 == 2:
                            break
                        last_err = f"HTTP {resp.status}"
            except Exception as e:
                last_err = str(e)
            time.sleep(1.5 * (attempt + 1))
        else:
            raise RuntimeError(f"HEC delivery failed after retries: {last_err}")
        self._sent += len(self._buf)
        self._buf.clear()

    @property
    def sent(self) -> int:
        return self._sent


# ----------------------------------------------------------------------------
# File (NDJSON) sink
# ----------------------------------------------------------------------------
class FileSink:
    def __init__(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        self.out_dir = out_dir
        self._fh = None
        self._cur_route = None
        self._written = 0

    def _rotate(self, route: str) -> None:
        if self._cur_route == route and self._fh is not None:
            return
        if self._fh is not None:
            self._fh.close()
        safe = route.replace("/", "_").replace("|", "_") or "unknown"
        path = os.path.join(self.out_dir, f"{safe}.ndjson")
        self._fh = open(path, "a", encoding="utf-8")
        self._cur_route = route

    def add(self, rec: Dict[str, Any]) -> None:
        self._rotate(rec.get("route", "unknown"))
        self._fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        self._written += 1

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    @property
    def written(self) -> int:
        return self._written


# ----------------------------------------------------------------------------
# Segment discovery
# ----------------------------------------------------------------------------
def discover_segments(input_path: str) -> List[str]:
    """Return a sorted list of segment files from a file, segment dir, or root."""
    if os.path.isfile(input_path):
        return [input_path]

    patterns = ["rlog", "rlog.bz2", "rlog.zst", "qlog", "qlog.bz2", "qlog.zst"]
    found: List[str] = []

    # a single segment directory?
    direct = [os.path.join(input_path, p) for p in patterns
              if os.path.isfile(os.path.join(input_path, p))]
    if direct:
        return sorted(direct)

    # a realdata root: <root>/<route>--<seg>/{rlog,qlog}
    for p in patterns:
        found.extend(glob.glob(os.path.join(input_path, "*", p)))
        found.extend(glob.glob(os.path.join(input_path, "*", "*", p)))
    # de-dupe: prefer rlog over qlog within the same segment dir
    by_dir: Dict[str, str] = {}
    for f in found:
        d = os.path.dirname(f)
        base = os.path.basename(f)
        if d not in by_dir or base.startswith("rlog"):
            by_dir[d] = f
    return sorted(by_dir.values())


def route_and_segment(seg_file: str) -> (str, str):
    """Derive (route, segment) labels from a segment file path."""
    seg_dir = os.path.basename(os.path.dirname(seg_file))
    # directory looks like <route>--<segnum> or <dongle>|<route>--<segnum>
    if "--" in seg_dir:
        route, _, seg = seg_dir.rpartition("--")
        return route or seg_dir, seg_dir
    return seg_dir, seg_dir


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ApexPilot -> Splunk log exporter")
    p.add_argument("--input", default=DEFAULT_REALDATA,
                   help=f"segment file, segment dir, or realdata root (default {DEFAULT_REALDATA})")
    p.add_argument("--out", default="./splunk_export",
                   help="output dir for NDJSON (file mode)")
    p.add_argument("--config", default=None, help="JSON config file")
    p.add_argument("--mode", choices=["file", "hec", "both"], default="file",
                   help="where to send events (default file = offline)")
    p.add_argument("--whitelist", default=None,
                   help="comma-separated message types to export (overrides config)")
    p.add_argument("--all", action="store_true",
                   help="export all message types except heavy tensor types")
    p.add_argument("--include-heavy", action="store_true",
                   help="also export heavy tensor types (camera/model). Not recommended.")
    p.add_argument("--hec-url", default=None, help="Splunk HEC base URL, e.g. https://host:8088")
    p.add_argument("--hec-token", default=None, help="Splunk HEC token")
    p.add_argument("--hec-index", default=None, help="Splunk index")
    p.add_argument("--sourcetype", default="apexpilot:log")
    p.add_argument("--source", default="apexpilot")
    p.add_argument("--no-verify-tls", action="store_true", help="skip TLS verification for HEC")
    p.add_argument("--limit", type=int, default=0, help="max events to export (0 = no limit)")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    cfg = load_config(args.config)

    # resolve whitelist
    if args.whitelist:
        whitelist = [s.strip() for s in args.whitelist.split(",") if s.strip()]
    elif cfg.get("whitelist"):
        whitelist = list(cfg["whitelist"])
    else:
        whitelist = list(DEFAULT_WHITELIST)
    whitelist_set = set(whitelist)
    export_all = args.all or bool(cfg.get("all"))

    segments = discover_segments(args.input)
    if not segments:
        sys.stderr.write(f"No rlog/qlog segments found under {args.input}\n")
        return 3
    sys.stderr.write(f"Found {len(segments)} segment(s).\n")

    log = _load_cereal()

    # set up sinks
    sinks = []
    file_sink = None
    hec_sink = None
    if args.mode in ("file", "both"):
        file_sink = FileSink(args.out)
        sinks.append(file_sink)
    if args.mode in ("hec", "both"):
        url = args.hec_url or cfg.get("hec", {}).get("url")
        token = args.hec_token or cfg.get("hec", {}).get("token")
        if not url or not token:
            sys.stderr.write("HEC mode requires --hec-url and --hec-token (or config).\n")
            return 2
        hec_sink = HECSink(
            url=url, token=token,
            index=args.hec_index or cfg.get("hec", {}).get("index"),
            sourcetype=args.sourcetype, source=args.source,
            verify_tls=not (args.no_verify_tls or cfg.get("hec", {}).get("no_verify_tls")),
        )
        sinks.append(hec_sink)

    total = 0
    skipped_big = 0
    try:
        for seg_file in segments:
            route, segment = route_and_segment(seg_file)
            try:
                for event in iter_messages(seg_file, log):
                    which = event.which()
                    if not export_all and which not in whitelist_set:
                        continue
                    if which in HEAVY_TYPES and not args.include_heavy:
                        continue
                    rec = event_to_record(event, route, segment)
                    if rec is None:
                        continue
                    # size guard
                    if len(json.dumps(rec, separators=(",", ":"))) > MAX_EVENT_BYTES:
                        skipped_big += 1
                        continue
                    for s in sinks:
                        s.add(rec)
                    total += 1
                    if args.limit and total >= args.limit:
                        raise StopIteration
            except StopIteration:
                break
            except Exception as e:
                sys.stderr.write(f"WARN: failed on {seg_file}: {e}\n")
                continue
    finally:
        for s in sinks:
            try:
                s.flush()
            except Exception as e:
                sys.stderr.write(f"ERROR flushing sink: {e}\n")
                if isinstance(s, HECSink):
                    return 4
        if file_sink:
            file_sink.close()

    sys.stderr.write(
        f"Done. exported={total} skipped_oversize={skipped_big}"
        + (f" file_written={file_sink.written}" if file_sink else "")
        + (f" hec_sent={hec_sink.sent}" if hec_sink else "")
        + "\n"
    )
    _ = _dt  # keep import for downstream edits
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
