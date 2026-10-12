# ApexPilot → Splunk log export

Turns openpilot/sunnypilot drive logs into Splunk events. **Offline-first:** the
default mode writes files and never touches the network, so it works on an
air-gapped device. HEC streaming is opt-in.

## How openpilot logs are laid out

On the device, drives are stored as *segments* (one per minute):

```
/data/media/0/realdata/
  <route>--0/          e.g. 0000008c--a1b2c3d4e5--0/
    rlog               full-rate capnp log  (or rlog.bz2 / rlog.zst when rotated)
    qlog               decimated capnp log
    qcamera.ts         (video, ignored by this tool)
```

Each `rlog`/`qlog` is a framed stream of [cereal](https://github.com/commaai/cereal)
capnp `Event` messages (`carState`, `controlsState`, `driverMonitoringState`, …).

## Install / prerequisites

Runs on the device or on any box that has the ApexPilot repo checked out (it
imports `cereal`). From the repo root:

```bash
# cereal must be importable; running from repo root usually suffices
export PYTHONPATH=$PWD
python3 integration/splunk/splunk_exporter.py --help
```

`zstandard` is only needed if your segments are `.zst` compressed:
`pip install zstandard --break-system-packages`. `requests` is optional (used for
HEC if present; otherwise it falls back to stdlib `urllib`).

## Offline export (default)

```bash
# Everything under realdata -> one NDJSON file per route in ./export
python3 splunk_exporter.py --input /data/media/0/realdata --out ./export

# A single route/segment, only the message types in the config
python3 splunk_exporter.py \
  --input /data/media/0/realdata/0000008c--a1b2c3d4e5--0 \
  --config splunk_config.json --out ./export
```

Output is newline-delimited JSON (`<route>.ndjson`). Each line:

```json
{"which":"carState","logMonoTime":51234567890,"valid":true,
 "route":"0000008c--a1b2c3d4e5","segment":"...--0",
 "data":{"vEgo":12.3,"steeringAngleDeg":1.2,"gasPressed":false, ...}}
```

### Getting the NDJSON into Splunk

Copy the files off the device (USB/SD/scp when you choose to), then either:

- **One-shot upload:** `splunk add oneshot export/<route>.ndjson -sourcetype _json -index apexpilot`
- **Monitor input:** point a `[monitor://.../export]` stanza with `sourcetype=_json`
  at the folder. `logMonoTime` is nanoseconds-since-boot; use the file mtime or a
  GPS/`liveLocationKalman` field for wall-clock if you need it.

## Online export (opt-in, needs network)

```bash
python3 splunk_exporter.py --input /data/media/0/realdata --mode hec \
  --hec-url https://splunk.internal:8088 --hec-token <TOKEN> --hec-index apexpilot
```

Events are batched (200/POST) with retry. Use `--mode both` to write files *and*
stream. `--no-verify-tls` for self-signed certs.

## Field selection & guards

- Default exports a **whitelist** of useful telemetry types (see `splunk_config.json`).
- `--all` exports everything *except* heavy tensor types (camera frames, `modelV2`).
- `--include-heavy` overrides that (will produce very large events — avoid).
- Lists longer than 64 entries are truncated; events over 64 KB are skipped and counted.

## In-UI entry point (optional)

To add a "Download logs for Splunk" button in the ApexPilot settings UI, wire a
menu action that runs the exporter and writes to a USB mount. Wire it into `selfdrive/ui/qt/offroad/settings.cc` as a `QProcess` action behind the existing developer/SSH gate, running `splunk_exporter.py --input /data/media/0/realdata --out /tmp/usb/export`.

## Notes

- This reads logs **after** a drive; it does not add any runtime overhead.
- Decimated `qlog` is usually enough for test review and is much smaller than `rlog`.
- The tool never deletes source logs.
