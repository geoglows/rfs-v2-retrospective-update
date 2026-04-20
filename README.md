# retrospective-update

Daily pipeline that downloads new ECMWF ERA5 runoff data, routes it through the GEOGLOWS v2 river network, and appends the resulting discharge to the public zarr archives on S3 (hourly, daily,
monthly-timeseries, monthly-timesteps) plus the HydroSOS GeoTIFF products.

The pipeline is designed to be idempotent and self-healing. It runs unattended from cron and is expected to either complete fully or abort with a clear error that a human investigates.

## Pipeline steps

All orchestrated by `main.sh`. Each step is bracketed by `START:` / `END:` banners in the log so you can grep a run's timeline.

1. **Environment setup** — source the per-machine env file (`variables.macstudio.env` or `variables.awsec2.env`), raise `ulimit -n`, activate conda, verify tools are on `PATH`.
2. **Download S3 copies** — `s5cmd sync` the static routing-configs directory every run (cheap, small file count). For each of the four zarr stores (hourly, daily, monthly-timeseries, monthly-timesteps) check whether the local `sentinel.json` is present; if not, `s5cmd sync --delete` from S3 so local exactly matches S3. Hourly / daily / monthly-timeseries skip the massive historical chunk `Q/0.*` since it never changes and is huge (S3 remains authoritative for those chunks).
3. **Prepare working directories** — clean the per-run working dirs (`discharge/`, `era5/`, `final-states/`, `forecast-inits/`, `hydrosos/`).
4. **Preflight validation** (`preflight_validation.py`) — aggregates three classes of check across all four zarrs and only exits non-zero if anything fails:
    - internal consistency (`.zarray` vs `.zmetadata`, time-dim sizes match time coord)
    - time coord integrity (no NaT, no duplicates, uniform spacing for hourly/daily, 28-31 day gaps for monthly)
    - local-vs-S3 sentinel comparison (see below)
5. **Download ERA5** (`download_era5.py`) — fetch all new daily hours from CDS, respecting `MIN_LAG_TIME_DAYS`.
6. **Routing** (`route.py`) — per-VPU Muskingum routing using `river-route`, parallelized across VPUs with a process pool.
7. **Upload init states and forecast inits** — push the fresh final-state and Qinit files to S3 first so downstream forecast systems can start as soon as possible.
8. **Append discharge** (`append_discharge.py`) — for each daily ERA5 output, concatenate across VPUs, load in memory, then append to `hourly.zarr` via xarray region writes on a dask `LocalCluster`.
   Daily is computed by resample+mean of the hourly data. Handles the edge cases we've hit: stale `.zmetadata`, unaligned dask vs zarr chunks along time, and xarray's region writes silently ignoring
   dim coords. After success, bumps each store's local sentinel.
9. **Upload hourly + daily zarrs** — Pattern A (see sentinels section). Body first with `--exclude "sentinel.json"`, then sentinel as the commit.
10. **Generate monthly products** (`monthly_products.py`) — compute monthly averages and HydroSOS classification COGs.
11. **Upload monthly products and HydroSOS COGs** — Upload for the two monthly zarrs; plain sync for the HydroSOS GeoTIFFs.
12. **Terminate** — post a completion message to the alerts webhook and optionally shuts down the machine.

`main.sh` accepts two flags:

- `--redownload-s3` — delete local zarr sentinels before starting, forcing an `s5cmd sync --delete` against S3 to make local match S3 exactly.
- `--local-is-truth <0|1>` — passed through to preflight. When `1`, skips all S3 comparisons and trusts local as long as the internal validations pass. Useful for running while getting behind on
  intentional divergence cases.

## Scheduling (launchd)

On the Mac Studio, the daily run is driven by a launchd agent rather than cron (so it survives reboots and picks up missed runs). The agent definition is committed as `rfs.v2.retrospective-updater.plist` — it wraps `main.sh` in `/usr/bin/lockf` to guard against concurrent runs and fires at 00:05 local time. The plist is static; edit it directly if the schedule or paths change.

Install or reload:

```sh
cp rfs.v2.retrospective-updater.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/rfs.v2.retrospective-updater.plist
# to reload after edits:
launchctl bootout gui/$(id -u)/rfs.v2.retrospective-updater
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/rfs.v2.retrospective-updater.plist
# run on demand:
launchctl kickstart -k gui/$(id -u)/rfs.v2.retrospective-updater
```

`StartCalendarInterval` uses the machine's local time, not UTC — verify the Mac Studio's TZ before trusting the schedule. The launchd-captured stdout/stderr at `/Users/Shared/logs/retrospective-update/launchd.{out,err}.log` only holds preamble before `main.sh`'s own `exec >> $LOG_FILE` takes over.

## Sentinels

Each of the four zarr stores carries a `sentinel.json` file living at its root. (The routing-configs directory does *not* have a sentinel — it's static reference data, resynced unconditionally every run.)

```json
{
  "updated": "2026-04-18T14:22:33Z"
}
```

The sentinel is a cheap, authoritative statement about "when this store was last written locally". It travels **inside** the store, so a single `s5cmd cp` round-trip moves both the data and its marker
together. Zarr itself ignores the file (it's not named `.zarray`, `.zgroup`, `.zattrs`, `.zmetadata`, or a chunk), so xarray / dask / zarr.consolidate_metadata never see it.

`main.sh` has a `write_sentinels <names>` bash function that writes the JSON directly. It's called after every successful local write (`append_discharge`, `monthly_products`). `preflight_validation.py` reads both sides (local file and S3 object) and compares them — see below.

Preflight's comparison is trivial — no timestamp tolerances, no clock-skew windows. Because Pattern A uploads the file verbatim, a successful sync means S3's sentinel is a byte-for-byte copy of local'
s:

| local vs S3                          | interpretation                                                  | action                                 |
|--------------------------------------|-----------------------------------------------------------------|----------------------------------------|
| `local == s3`                        | fully synced                                                    | PASS                                   |
| `local.updated > s3.updated`         | local has new work, upload is pending or was interrupted        | WARN; pipeline will re-upload          |
| `local.updated < s3.updated`         | S3 moved forward without us knowing (admin push, other machine) | ERROR + alert; human decides           |
| same `updated` but different content | race or corruption                                              | ERROR + alert                          |
| S3 sentinel missing                  | first-run bootstrap                                             | WARN, proceed; pipeline will create it |

### Pattern A upload

`sentinel.json` is uploaded **last**, after the zarr body is on S3. This makes the sentinel upload the atomic "commit":

```bash
s5cmd cp --exclude "sentinel.json" "$HOURLY_ZARR/*" "$S3_HOURLY_ZARR/"    # body
s5cmd cp "$HOURLY_ZARR/sentinel.json" "$S3_HOURLY_ZARR/sentinel.json"    # commit
```

If the body upload dies halfway (network drop, thread exhaustion), S3's sentinel still reflects the previous synced revision and the next preflight detects "local is ahead" — the pipeline
transparently resumes by re-uploading. If the sentinel upload specifically fails, same outcome. We never end up with a "looks synced but actually missing chunks" state.

### Bootstrapping

The first time the pipeline is deployed against a set of S3 zarrs that predate the sentinel scheme, run `bootstrap_sentinels.py` once. It declares local as revision 1 and pushes a matching sentinel to
each S3 store.

```sh
source variables.macstudio.env
python bootstrap_sentinels.py
```

After that, every subsequent `main.sh` run carries sentinels forward automatically.

## Scheduling on the Mac Studio (launchd)

The Mac Studio runs the pipeline via a **LaunchDaemon** rather than cron. Cron on macOS tags child processes with `PRIO_DARWIN_BG`, clamping them to efficiency cores and throttling I/O; launchd lets the plist declare the opposite. The daemon also drops privileges from root → `rchales` so CrowdStrike Falcon (and anything similar) treats file operations as user-owned work, not privileged — a 10× speedup in practice.

The plist lives at `/Users/Shared/code/rfs-v2-retrospective-update/rfs.v2.retrospective-updater.plist` in the repo. It declares:

- `UserName = rchales`, `GroupName = staff` — launchd drops privileges before exec'ing `main.sh`
- `ProcessType = Interactive` + `LowPriorityIO = false` — no QoS or I/O throttling
- `StartCalendarInterval` — fires daily at **00:05 machine-local time** (verify the clock is UTC before relying on "00:05 UTC")
- Wraps the invocation in `/usr/bin/lockf` against `/Users/Shared/lockfiles/retrospective-update.lock` so overlapping runs are prevented

### Install

```sh
sudo cp /Users/Shared/code/rfs-v2-retrospective-update/rfs.v2.retrospective-updater.plist /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo chown root:wheel /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo chmod 644      /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo launchctl load -w /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
```

`launchctl load -w` both loads the plist and persists its enabled-at-boot state so it survives reboots.

Verify it's registered:

```sh
sudo launchctl list | grep retrospective
# PID column is '-' until the next 00:05 fire; that's normal
```

### Trigger a run immediately (don't wait for the schedule)

```sh
sudo launchctl kickstart -k system/rfs.v2.retrospective-updater
```

The `-k` kills any currently-running instance first and starts a fresh one. Drop `-k` if you want "start only if nothing is running".

### Reload after editing the plist

```sh
sudo launchctl unload /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo cp /Users/Shared/code/rfs-v2-retrospective-update/rfs.v2.retrospective-updater.plist /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo launchctl load -w /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
```

### Uninstall

```sh
sudo launchctl unload /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
sudo rm /Library/LaunchDaemons/rfs.v2.retrospective-updater.plist
```

### File ownership prerequisites

For the daemon (running as `rchales:staff`) to succeed, every path it touches must be readable/writable by that uid/gid. After transitioning from the old root-cron setup, run once:

```sh
sudo chown -R rchales:staff /Users/Shared/workdirs/rfs-v2-retrospective-update
sudo chown -R rchales:staff /Users/Shared/logs/retrospective-update
sudo chown -R rchales:staff /Users/Shared/environments/rfs-v2-retrospective-update
sudo chown -R rchales:staff /Users/Shared/code/rfs-v2-retrospective-update
sudo mkdir -p /Users/Shared/lockfiles
sudo chown rchales:staff /Users/Shared/lockfiles
sudo rm -f /Users/Shared/lockfiles/retrospective-update.lock   # let daemon recreate it fresh
```

### Troubleshooting

- **Nothing ran at the scheduled time.** Check `sudo launchctl list | grep retrospective`. If the label isn't listed, the plist isn't loaded — re-run the install `launchctl load -w` line. If the label is listed but the last exit code is non-zero, check `launchd.err.log` and `launchd.out.log` under `/Users/Shared/logs/retrospective-update/` for pre-main.sh crashes.
- **"Permission denied" somewhere in the pipeline.** Something was created by a previous root run and still root-owned. Use `sudo find /Users/Shared/workdirs/rfs-v2-retrospective-update -user root` to locate, then `chown -R rchales:staff` those paths.
- **Pipeline still slow.** Confirm it's actually launchd firing it and not the old root cron: `ps -eo pid,user,command | grep main.sh`. The user column should show `rchales`, not `root`.

## Layout

```
main.sh                                    # orchestrator
rollback_zarrs.py                          # recovery tool: roll zarrs back to a target timestamp in parallel
rfs.v2.retrospective-updater.plist         # Mac Studio LaunchDaemon (installs to /Library/LaunchDaemons/)
variables.macstudio.env                    # per-machine env file
variables.awsec2.env                       # per-machine env file
retrospective-update/
    preflight_validation.py     # step 4
    download_era5.py            # step 5
    route.py                    # step 6
    append_discharge.py         # step 8
    monthly_products.py         # step 10
    validators.py               # shared internal-consistency + time-coord checks
    cloud_logger.py             # webhook posting
    set_env_vars.py             # Python-side env var imports
```

## Recovery

When a pipeline run fails midway you have a few tools:

- **Re-run `main.sh`**: If the failure was transient (network, thread exhaustion), the sentinel comparison in preflight correctly identifies "local ahead" and the pipeline skips the already-done work,
  picking up at the failed step.
- **`main.sh --local-is-truth 1`**: Use when you know local is correct but the preflight's S3 comparison is noisy (e.g., deliberate manual changes were made).
- **`main.sh --redownload-s3`**: Nuclear option. Deletes local zarr sentinels so each `if ! -f sentinel.json` block triggers and runs `s5cmd sync --delete` against S3, making local match S3 exactly. Use when local is corrupt or you want to discard everything since the last S3 upload.
- **`rollback_zarrs.py`**: Surgical rollback of each zarr to a specific target time, with parallel chunk cleanup. Use after partial appends have left zarr arrays in an inconsistent size/shape state.
