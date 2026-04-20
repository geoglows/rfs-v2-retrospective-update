# retrospective-update

Daily pipeline that downloads new ECMWF ERA5 runoff data, routes it through the GEOGLOWS v2 river network, and appends the resulting discharge to the public zarr archives on S3 (hourly, daily,
monthly-timeseries, monthly-timesteps) plus the HydroSOS GeoTIFF products.

The pipeline is designed to be idempotent and self-healing. It runs unattended from cron and is expected to either complete fully or abort with a clear error that a human investigates.

## Pipeline steps

All orchestrated by `main.sh`. Each step is bracketed by `START:` / `END:` banners in the log so you can grep a run's timeline.

1. **Environment setup** — source the env file, raise `ulimit -n`, activate conda, verify tools are on `PATH`.
2. **Download S3 copies**
    - `s5cmd sync` the routing-configs every run.
    - For each of four zarrs (hourly, daily, monthly-timeseries, monthly-timesteps), check if `sentinel.json` is present; if not, `s5cmd sync --delete` from S3 excluding the "Q/0.*" chunks.
3. **Prepare working directories** — clean per-run working dirs (`discharge/`, `final-states/`, `forecast-inits/`, `hydrosos/`).
    - Note: `era5/` is not cleared because redownloading is slow and to allow providing the data. Other checks need to verify date ranges before trusting the provided data.
4. **Preflight validation** (`preflight_validation.py`) — checks all four zarrs in the following ways:
    - Is there a sentinel.json (should exist per step 2)?
        - Is the sentinel's timestep < sentinel on S3? (if `--local-is-truth`, trust it. If not, error because we need to pick up where s3 was)
        - Is the sentinel's timstep = sentinel on s3? (good, intended case)
        - Is the sentinel's timestep > sentinel on s3? (warn, a partial update occurred but local is ahead so it should fix s3 by end of the job)
    - internal consistency of the zarrs
        - Do`.zarray` and `.zmetadata` align
        - Does the time-dim size match time coord
    - time coord integrity (no NaT, no duplicates, uniform spacing for hourly/daily, 28-31 day gaps for monthly)
5. **Download ERA5** (`download_era5.py`) — fetch all new hourly values up until `MIN_LAG_TIME_DAYS` ago.
6. **Routing** (`route.py`) — spawns a process pool, routes several VPUs concurrently, writes:
    - discharges
    - final states (per river-route style)
    - converts final state to RAPID style for forecast inits (Qinit)
7. **Upload init states and forecast inits** — push the fresh final-state and Qinit files to S3 first so downstream forecast systems can start as soon as possible.
8. **Append Hourly and Daily Discharge** (`append_discharge.py`) — concatenate discharge across VPUs and append many chunks concurrently with xarray region writes and a dask `LocalCluster`.
   Daily is computed by resample+mean of the hourly data and appended the same way. After successfully appending, update the zarr's sentinel.
9. **Upload hourly + daily zarrs** — Body first with `--exclude "sentinel.json"`, then sentinel as a commit.
10. **Generate monthly products** (`monthly_products.py`) — compute monthly averages and HydroSOS classification COGs, only if a new month has passed.
11. **Upload monthly products and HydroSOS COGs** — Upload the monthly zarrs and geotiffs to s3. Sentinels in the zarr go last.
12. **Terminate** — post a completion message to the alerts webhook and optionally shuts down the machine.

`main.sh` accepts two flags:

- `--redownload-s3` — delete local zarr sentinels before starting to cause `s5cmd sync --delete` to make the local exactly match s3. Use if local zarrs are corrupt but s3 is good.
- `--local-is-truth` — Skips comparing sentinels between local and s3, only check if the zarr is valid before appending. Use if s3 is corrupted and you're providing a known-good local copy.

## Known attainable bad states

The following are the common possible invalid states, if they are recoverable, and how they are fixed.

| scenario                                 | interpretation                                                  | action                                                                               |
|------------------------------------------|-----------------------------------------------------------------|--------------------------------------------------------------------------------------|
| local zarr sentinel ahead of S3          | local has new work, upload is pending or was interrupted        | WARN; next successful run will correct it without intervention                       |
| local zarr sentinel behind S3            | S3 moved forward without us knowing (admin push, other machine) | ERROR + alert; use --redownload-s3 if s3 copy is trustable                           |
| same sentinel timestamp but zarr corrupt | Incomplete append, copy, upload                                 | ERROR + alert; may need --redownload-s3, or rollback zarr and use --local-is-truth   |
| S3 sentinel missing, local has it        | Sentinel file deleted by some process                           | ERROR + alert, upload local sentinel, use --local-is-truth if local copy trustable   |
| Local sentinel missing, S3 has it        | Sentinel file deleted by some process                           | ERROR + alert, download S3 sentinel, use --redownload-s3 if s3 copy trustable        |
| Both sentinels missing                   | Sentinel file deleted by some process                           | ERROR + alert, place sentinel in local + s3, use --local-is-truth or --redownload-s3 |
| Duplicate time steps in zarr             | Most likely, duplicate era5 values were routed                  | ERROR + alert; delete `era5/`, use `rollback_zarrs.py`, use --local-is-truth         |
| NaT time steps in zarr                   | Interrupted append or invalid append attempted                  | ERROR + alert; delete `era5/`, use `rollback_zarrs.py`, use --local-is-truth         |

## Scheduling (launchd)

The daily compute is schedules with cron or launchd (`rfs.v2.retrospective.plist`). Jobs invoke a lockfile first so multiple runs cannot start and try to modify the same zarr simultaneously.

Job should launch at 00:05 UTC daily. Verify the machine's timezone or otherwise arrange for scheduler to use UTC.

Install or reload:

```sh
cp rfs.v2.retrospective.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/rfs.v2.retrospective.plist
# to reload after edits:
launchctl bootout gui/$(id -u)/rfs.v2.retrospective
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/rfs.v2.retrospective.plist
# run on demand:
launchctl kickstart -k gui/$(id -u)/rfs.v2.retrospective
```

The `launchd` plist is given in `rfs.v2.retrospective.plist` in the repo. It declares:

- `UserName = rchales`, `GroupName = staff` — launchd drops privileges before exec'ing `main.sh`
- `ProcessType = Interactive` + `LowPriorityIO = false` — no QoS or I/O throttling
- `StartCalendarInterval` — fires daily at **00:05 machine-local time** (verify the clock is UTC before relying on "00:05 UTC")
- Wraps the invocation in `/usr/bin/lockf` against `/Users/Shared/lockfiles/retrospective-update.lock` so overlapping runs are prevented

### Install

```sh
sudo cp /Users/Shared/code/rfs-v2-retrospective-update/rfs.v2.retrospective.plist /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo chown root:wheel /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo chmod 644      /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo launchctl load -w /Library/LaunchDaemons/rfs.v2.retrospective.plist
```

`launchctl load -w` both loads the plist and persists its enabled-at-boot state so it survives reboots.

Verify it's registered:

```sh
sudo launchctl list | grep rfs.v2
# PID column is '-' until the next 00:05 fire; that's normal
```

### Trigger a run immediately (don't wait for the schedule)

```sh
sudo launchctl kickstart -k system/rfs.v2.retrospective
```

The `-k` kills any currently-running instance first and starts a fresh one.

### Reload after editing the plist

```sh
sudo launchctl unload /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo cp /Users/Shared/code/rfs-v2-retrospective-update/rfs.v2.retrospective.plist /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo launchctl load -w /Library/LaunchDaemons/rfs.v2.retrospective.plist
```

### Uninstall

```sh
sudo launchctl unload /Library/LaunchDaemons/rfs.v2.retrospective.plist
sudo rm /Library/LaunchDaemons/rfs.v2.retrospective.plist
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

## Sentinels

Each of the four zarr stores carries a `sentinel.json` file living at its root. (The routing-configs directory does *not* have a sentinel — it's static reference data, resynced unconditionally every
run.)

```json
{
  "updated": "2026-04-18T14:22:33Z"
}
```

## Layout

```
main.sh                            # orchestrator
rfs.v2.retrospective.plist         # Mac Studio LaunchDaemon (installs to /Library/LaunchDaemons/)
variables.macstudio.env            # per-machine env file
variables.awsec2.env               # per-machine env file
retrospective-update/
    preflight_validation.py        # step 4
    download_era5.py               # step 5
    route.py                       # step 6
    append_discharge.py            # step 8
    monthly_products.py            # step 10
    helpers/
        validators.py              # shared internal-consistency + time-coord checks
        cloud_logger.py            # webhook posting
        set_env_vars.py            # Python-side env var imports
        rollback_zarrs.py          # recovery tool: roll zarrs back to a target timestamp in parallel
        prepare_hydrosos_thresholds.py
```

## Recovery

When a pipeline run fails midway you have a few tools:

- **Re-run `main.sh`**: If the failure was transient (network, thread exhaustion), the sentinel comparison in preflight correctly identifies "local ahead" and the pipeline skips the already-done work,
  picking up at the failed step.
- **`main.sh --local-is-truth 1`**: Use when you know local is correct but the preflight's S3 comparison is noisy (e.g., deliberate manual changes were made).
- **`main.sh --redownload-s3`**: Nuclear option. Deletes local zarr sentinels so each `if ! -f sentinel.json` block triggers and runs `s5cmd sync --delete` against S3, making local match S3 exactly.
  Use when local is corrupt or you want to discard everything since the last S3 upload.
- **`rollback_zarrs.py`**: Surgical rollback of each zarr to a specific target time, with parallel chunk cleanup. Use after partial appends have left zarr arrays in an inconsistent size/shape state.
