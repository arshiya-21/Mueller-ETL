"""
Mueller 150T mixer ETL.

Flow:
    [rct table]
        │  Stage 1 — cursor "rct" (last rct.mmid processed)
        │  read_events → build_batches → build_data_frame → insert_mixer
        ▼
    [mixer_cycle_data]
        │  Stage 2 — cursor "mixer" (last mixer_cycle_data.id sent)
        │  fetch_mixer_rows → build_smc_payload → upload_smc
        ▼
    [SMC API /upload/prepared_sand_extra_upload_bulk]

Each stage advances its cursor in the same transaction as its side-effect
(the insert, or the cursor-only UPDATE after a successful HTTP batch), so a
crash between the two never produces duplicates or lost rows.
"""

import argparse
import atexit
import datetime as dt
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import requests
import urllib3
from sqlalchemy import bindparam, create_engine, text
from sshtunnel import SSHTunnelForwarder

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =========================================================================
# Logging — one file per day in ./logs/, retention = 3 days.
#   File name:  smc_<D>_<M>_<YY>.log   (e.g. smc_1_11_26.log for 2026-11-01)
#   Rotation:   setup_logger() runs at the start of every cycle; on the first
#               cycle past midnight the handler re-points at the new day's
#               file and prunes anything older than 3 days.
# =========================================================================

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("mueller_etl")
logger.setLevel(logging.INFO)
_logger_date = None


def setup_logger():
    """Point the logger at today's file; prune files older than 3 days."""
    global _logger_date
    today = dt.date.today()
    if _logger_date == today and logger.handlers:
        return

    # Keep only the 3 most recent log files; delete the 4th-oldest and beyond.
    files = sorted(LOG_DIR.glob("smc_*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
    for f in files[3:]:
        try:
            f.unlink()
        except OSError:
            pass

    for h in list(logger.handlers):
        logger.removeHandler(h); h.close()

    fname = f"smc_{today.day}_{today.month}_{today.year % 100}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_DIR / fname, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    _logger_date = today


setup_logger()
log = logger.info


# =========================================================================
# Config
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
# Prefer committed config.json; fall back to config.example.json on fresh
# deployments (e.g. Railway, where config.json is gitignored and secrets
# arrive via env vars).
_cfg_path = BASE_DIR / "config.json"
if not _cfg_path.exists():
    _cfg_path = BASE_DIR / "config.example.json"
CFG = json.loads(_cfg_path.read_text("utf-8"))

# Env-var overrides for secrets (used in Railway).
CFG["database"]["password"] = os.getenv("DB_PASSWORD", CFG["database"]["password"])
CFG["api"]["j_password"] = os.getenv("API_PASSWORD", CFG["api"]["j_password"])

# SSH key: Railway can't carry a file, so SSH_PRIVATE_KEY holds the key's full
# contents. Write it to a temp file so sshtunnel/paramiko can still read it as
# a path (same pattern as AIW/production_booking.py).
if os.environ.get("SSH_PRIVATE_KEY"):
    _key_path = os.path.join(tempfile.gettempdir(), "mueller_ssh_key.pem")
    with open(_key_path, "w") as _kf:
        _kf.write(os.environ["SSH_PRIVATE_KEY"])
    try:
        os.chmod(_key_path, 0o600)
    except OSError:
        pass  # Windows ignores chmod
    CFG["ssh_tunnel"]["ssh_private_key_path"] = _key_path

DB, SSH, API = CFG["database"], CFG["ssh_tunnel"], CFG["api"]
RCT_TABLE, MIXER_TABLE, STATE_TABLE = CFG["rct_table"], CFG["mixer_table"], CFG["sync_state_table"]
POLL_INTERVAL = int(CFG.get("poll_interval_seconds", 600))

CYCLE_ADDR  = CFG["cycle_address"]
TAIL_SECS   = CFG["tail_seconds"]
TIME_OFFSET = CFG["cycle_time_offset"]
TARGET_MIX  = CFG["target_mix_time"]
COMP_OFFSET = CFG["comp_setpoint_offset"]
INCLUDE_INCOMPLETE = CFG["include_incomplete"]

# address -> (db_column, divisor). Lookup is O(1) at use.
ADDRESS_MAP = {e["address"]: (e["db_column"], float(e.get("divisor") or 1))
               for e in CFG["plc_mapping"]}
assert CYCLE_ADDR in ADDRESS_MAP, f"cycle_address {CYCLE_ADDR} missing from plc_mapping"
CYCLE_NO_COL = ADDRESS_MAP[CYCLE_ADDR][0]
MAPPED_ADDRS = tuple(ADDRESS_MAP)

# Precompile shift windows once (O(k) per lookup, k = 2–3 shifts).
SHIFTS = [(int(k) if str(k).isdigit() else k,
           *[dt.time.fromisoformat(p.strip()) for p in v.split("-")])
          for k, v in CFG["shift_timings"].items()]

MIXER_COLUMNS = [
    "cycle_date", "cycle_time", "cycle_no",
    "compact_preset_pct", "sand_input_temp_c", "corrected_preset_pct",
    "moisture_measure_pct", "readjust_water_l",
    "compact_as1_measure_pct", "corrected_water_2_l",
    "compact_as2_measure_pct", "corrected_water_3_l",
    "compact_as3_measure_pct", "corrected_water_4_l",
    "variation_pct", "total_water_l", "cycle_time_sec",
    "c_strength_preset_n_cm2", "c_strength_measure_n_cm2",
    "weight_bento_kg", "additif2", "additif3", "additif4",
    "weight_old_sand_kg", "poids_sn", "compact_as_final_measure_pct",
    "temp_sortie_c", "pair_as", "pair_rc", "pair_al",
    "run_no", "dcy", "week_no",
    "comp_setpoint_minus3", "comp_setpoint_plus3", "target_mix_time_sec",
]


# =========================================================================
# Infrastructure: SSH tunnel, DB engine, one-time schema setup
# =========================================================================

def _make_tunnel():
    return SSHTunnelForwarder(
        (SSH["ssh_host"], SSH.get("ssh_port", 22)),
        ssh_username=SSH["ssh_username"],
        ssh_pkey=SSH["ssh_private_key_path"],
        ssh_private_key_password=SSH.get("ssh_private_key_password") or None,
        remote_bind_address=(DB["host"], int(DB["port"])),
        set_keepalive=30,
    )


def _make_engine():
    return create_engine(
        f"mysql+pymysql://{quote_plus(DB['user'])}:{quote_plus(DB['password'])}"
        f"@127.0.0.1:{tunnel.local_bind_port}/{DB['database_name']}",
        pool_pre_ping=True,
        connect_args={"read_timeout": 1200, "write_timeout": 1200, "connect_timeout": 60},
    )


tunnel = _make_tunnel()
tunnel.start()
atexit.register(tunnel.stop)
engine = _make_engine()


def ensure_tunnel():
    """Reopen the SSH tunnel if the AWS bastion dropped the session during
    our 10-min idle sleep. Called at the start of each cycle."""
    global tunnel, engine
    try:
        if tunnel.is_active and tunnel.is_alive:
            return
    except Exception:
        pass
    log(f"SSH tunnel is down; reopening...")
    try:
        tunnel.stop()
    except Exception:
        pass
    tunnel = _make_tunnel()
    tunnel.start()
    engine.dispose()
    engine = _make_engine()


def _init_schema():
    """Create cursor table and seed rows once at startup (not per cycle)."""
    with engine.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                last_id INT NOT NULL DEFAULT 0,
                source_table VARCHAR(32) NOT NULL UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """))
        for src in ("rct", "mixer"):
            c.execute(text(f"INSERT IGNORE INTO {STATE_TABLE} (source_table) VALUES (:s)"),
                      {"s": src})


_init_schema()


def get_cursor(conn, source):
    row = conn.execute(
        text(f"SELECT last_id FROM {STATE_TABLE} WHERE source_table = :s"),
        {"s": source}).fetchone()
    return int(row[0])


def set_cursor(conn, source, last_id):
    conn.execute(
        text(f"UPDATE {STATE_TABLE} SET last_id = :i WHERE source_table = :s"),
        {"i": int(last_id), "s": source})


# =========================================================================
# Shift / time helpers
# =========================================================================

def to_time(v):
    """TIME columns return as timedelta from PyMySQL; normalize to dt.time."""
    if isinstance(v, dt.timedelta):
        s = int(v.total_seconds())
        return dt.time(s // 3600, (s % 3600) // 60, s % 60)
    return v


def shift_for(t):
    t = to_time(t)
    for label, start, stop in SHIFTS:
        wraps = start > stop
        if (t >= start or t < stop) if wraps else (start <= t < stop):
            return label
    return None


def excel_weeknum(d):
    offset = (d.replace(month=1, day=1).weekday() + 1) % 7
    return (d.timetuple().tm_yday - 1 + offset) // 7 + 1


# =========================================================================
# Stage 1: rct → mixer_cycle_data
# =========================================================================

def read_events(conn, last_mmid):
    """One query, two outputs: the mapped-address events (oldest first) and
    the overall max mmid past the cursor (so unmapped tail rows don't stall it).
    overall_max comes via a UNION-ALL sentinel row rather than a second trip."""
    stmt = text(f"""
        SELECT mmid, entered, address, string2 FROM {RCT_TABLE}
        WHERE mmid > :last AND address IN :addrs
          AND entered IS NOT NULL AND string2 IS NOT NULL
        ORDER BY entered, mmid
    """).bindparams(bindparam("addrs", expanding=True))
    rows = conn.execute(stmt, {"last": last_mmid, "addrs": list(MAPPED_ADDRS)}).fetchall()

    max_mmid = conn.execute(
        text(f"SELECT MAX(mmid) FROM {RCT_TABLE} WHERE mmid > :last"),
        {"last": last_mmid}).scalar() or last_mmid

    events = []
    for mmid, entered, addr, s2 in rows:
        try:
            events.append((entered, int(mmid), addr.strip(), float(s2)))
        except (TypeError, ValueError):
            pass
    return events, int(max_mmid)


def build_batches(events):
    """Single pass O(n). A cycle_address event closes a batch; following
    events within TAIL_SECS stick to it (late results)."""
    tail = dt.timedelta(seconds=TAIL_SECS)
    state, batches = {}, []
    start, closing, seen = None, None, False

    def emit(end, start_t):
        batches.append({
            "end": end, "complete": start_t is not None,
            "values": {col: (state.get(a, 0) / d if d != 1 else state.get(a, 0))
                       for a, (col, d) in ADDRESS_MAP.items()},
        })

    for ts, _mmid, addr, val in events:
        if closing and ts - closing[0] > tail:
            emit(*closing); closing, start = None, None
        if addr == CYCLE_ADDR:
            if closing:
                emit(*closing)
            state[addr] = val
            closing, start, seen = (ts, start if seen else None), None, True
            continue
        if not closing and not start:
            start = ts
        state[addr] = val

    if closing:
        emit(*closing)
    return batches


def build_data_frame(batches):
    rows, run_day, run_no, prev_ct = [], None, 0, None
    for b in batches:
        shifted = None if prev_ct is None else prev_ct + TIME_OFFSET
        prev_ct = b["values"]["cycle_time_sec"]

        if not (b["complete"] or INCLUDE_INCOMPLETE):
            continue

        end = b["end"]
        run_no = run_no + 1 if end.date() == run_day else 1
        run_day = end.date()
        preset = round(b["values"]["compact_preset_pct"], 1)

        row = dict.fromkeys(MIXER_COLUMNS, 0)
        for col, v in b["values"].items():
            row[col] = round(v, 1)
        row.update(
            cycle_date=end.date(),
            cycle_time=end.time().replace(microsecond=0),
            cycle_no=int(b["values"][CYCLE_NO_COL]),
            cycle_time_sec=shifted,
            compact_preset_pct=preset,
            comp_setpoint_minus3=round(preset - COMP_OFFSET, 1),
            comp_setpoint_plus3=round(preset + COMP_OFFSET, 1),
            target_mix_time_sec=TARGET_MIX,
            run_no=run_no,
            week_no=excel_weeknum(end.date()),
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=MIXER_COLUMNS)


def insert_mixer(conn, df):
    """Insert on the given connection so the caller can advance the cursor
    in the same transaction."""
    if df.empty:
        return 0
    out = df.copy()
    out["cycle_time"] = out["cycle_time"].map(lambda t: to_time(t).strftime("%H:%M:%S"))
    out.to_sql(MIXER_TABLE, conn, if_exists="append", index=False,
               chunksize=500, method="multi")
    return len(out)


def stage_rct_to_mixer():
    with engine.begin() as conn:
        cursor = get_cursor(conn, "rct")
        events, max_mmid = read_events(conn, cursor)
        df = build_data_frame(build_batches(events))
        log(f"rct cursor={cursor}, events={len(events)}, data_df rows={len(df)}")
        try:
            n = insert_mixer(conn, df)
            if max_mmid > cursor:
                set_cursor(conn, "rct", max_mmid)
            log(f"Inserted {n} row(s); rct cursor -> {max_mmid}")
        except Exception as exc:
            # Transaction rolls back on exception; cursor stays put.
            orig = getattr(exc, "orig", exc)
            log(f"Stage 1 failed: {type(orig).__name__}: {orig}; rct cursor NOT advanced.")
            raise


# =========================================================================
# Stage 2: mixer_cycle_data → SMC API
# =========================================================================

# smc_query.sql is kept verbatim (same snake_case shape as AIW/Crescent).
# We wrap it in a subquery here to (a) attach mixer_cycle_data.id via a join
# — so the cursor is a plain row id matching what you see in Workbench —
# and (b) filter by that id. The file itself stays untouched.
_SMC_RAW = (Path(__file__).resolve().parent / "smc_query.sql").read_text("utf-8").strip().rstrip(";")
SMC_SQL = text(f"""
    SELECT m.id AS _mixer_id, smc.* FROM (
    {_SMC_RAW}
    ) smc
    JOIN {MIXER_TABLE} m
      ON m.cycle_date = smc.`date` AND m.cycle_time = smc.`time`
    WHERE m.id > :last
    ORDER BY m.id
""")

# snake_case (SQL alias) -> camelCase (PreparedSandExtra bean / Jackson default).
# Same set AIW uses: Crescent's 8 fields + temp_st1c -> temp + wd1 (Mueller's
# smc_query exposes both, and the server bean has setters for them).
FIELD_MAP = {
    "co1":                 "co1",
    "co_final_percentage": "coFinalPercentage",
    "cosp_percent":        "cospPercentage",
    "date":                "date",
    "shift":               "shift",
    "temp_st1c":           "temp",
    "time":                "time",
    "total_seconds":       "totalSeconds",
    "total_water":         "totalWater",
    "wd1":                 "wd1",
}


def fetch_smc_rows(conn, last_cursor):
    return conn.execute(SMC_SQL, {"last": last_cursor}).mappings().all()


def build_smc_payload(rows):
    """Mirrors Crescent's rows_to_json structure (dict-comp over FIELD_MAP via
    an inner to_val) and adds AIW's foundryLine={"pkey": ...} wrapper."""

    def to_val(field, v):
        if field == "date":
            return v if isinstance(v, str) else v.strftime("%Y-%m-%d 00:00:00")
        if field == "time":
            return to_time(v).strftime("%H:%M:%S")
        if field == "shift":
            # PreparedSandExtra.shift is a String — AIW/Crescent send 'A'/'B'/'C'.
            # Our SQL returns INT (1/3); cast to str so Jackson can bind it.
            return str(v)
        return float(v)

    # Don't send foundryLine — the controller falls back to the authenticated
    # user's own customer line (readOnlyAllowedFoundryLinesForCustomer) when
    # smcFoundry is null, which is safer than hardcoding a pkey that varies
    # per tenant. If a specific pkey is ever needed, re-enable foundry below.
    payload = []
    for row in rows:
        entry = {FIELD_MAP[f]: to_val(f, row[f])
                 for f in FIELD_MAP
                 if row.get(f) is not None}
        payload.append(entry)
    return payload


# --- API session ---------------------------------------------------------

_session = requests.Session()
_session.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36")
_jsessionid = None


def api_login():
    """Same as AIW/Crescent: POST creds to j_spring_security_check and trust
    the resulting JSESSIONID. Spring picks the landing URL based on role —
    not something to control from the client."""
    global _jsessionid
    r = _session.post(API["base_url"] + API["login_url"],
                      data={"j_username": API["j_username"], "j_password": API["j_password"]},
                      verify=False, timeout=30, allow_redirects=True)
    _jsessionid = _session.cookies.get("JSESSIONID")
    ok = r.status_code == 200 and bool(_jsessionid)
    log(f"API login: HTTP {r.status_code} | final={r.url} | authenticated={ok}")
    return ok


def _post(payload):
    """Let the Session cookie jar handle cookies (don't override Cookie header
    — the server needs more than just JSESSIONID). Pass X-CSRF-TOKEN too,
    exactly like Crescent's upload_batch does. allow_redirects=False so a 302
    to Home is surfaced instead of silently followed."""
    headers = {"Content-Type": "application/json"}
    xsrf = _session.cookies.get("XSRF-TOKEN")
    if xsrf:
        headers["X-CSRF-TOKEN"] = xsrf
    return _session.post(
        API["base_url"] + API["upload_url"], json=payload, verify=False, timeout=120,
        headers=headers, allow_redirects=False,
    )


def _is_authenticated_response(r):
    """A real upload response is 2xx with a JSON (or plain-text) body. A 3xx
    or an HTML page in the body means Spring sent us back to Home / login."""
    if 300 <= r.status_code < 400:
        return False
    if r.status_code in (401, 403):
        return False
    if "<html" in r.text[:500].lower() or "<!doctype" in r.text[:500].lower():
        return False
    return r.ok


def post_batch(payload):
    if _jsessionid is None and not api_login():
        return False
    r = _post(payload)
    if not _is_authenticated_response(r):
        log(f"Not authenticated (HTTP {r.status_code}); re-logging in and retrying.")
        if not api_login():
            return False
        r = _post(payload)
    ok = _is_authenticated_response(r)
    log(f"Upload: HTTP {r.status_code} | ok={ok} | body={r.text[:200]!r}")
    return ok


def upload_smc(payload):
    size = int(API.get("batch_size", 100))
    if payload:
        log(f"First record being sent: {json.dumps(payload[0])}")
    for i in range(0, len(payload), size):
        chunk = payload[i:i + size]
        if not post_batch(chunk):
            log(f"SMC batch {i // size + 1} failed; stopping.")
            return False
        log(f"SMC batch {i // size + 1}: {len(chunk)} row(s) uploaded.")
    return True


def stage_mixer_to_smc():
    with engine.connect() as conn:
        cursor = get_cursor(conn, "mixer")
        rows = fetch_smc_rows(conn, cursor)
    log(f"mixer cursor={cursor}, rows to send={len(rows)}")
    if not rows:
        return

    if not upload_smc(build_smc_payload(rows)):
        log("SMC API upload failed; mixer cursor NOT advanced.")
        return

    # Wrapper SELECT orders by mixer_cycle_data.id, so the last row's _mixer_id
    # is the furthest id we've sent. The cursor is now a plain row id, matching
    # what you see in Workbench on mixer_cycle_data.id.
    new_cursor = int(rows[-1]["_mixer_id"])
    with engine.begin() as conn:
        set_cursor(conn, "mixer", new_cursor)
    log(f"mixer cursor -> {new_cursor}")


# =========================================================================
# Loop
# =========================================================================

def run_cycle():
    setup_logger()  # Rolls to tomorrow's file after midnight; prunes old files.
    ensure_tunnel()
    try:
        stage_rct_to_mixer()
    except Exception:
        pass  # Already logged; keep running so stage 2 can still catch up.
    stage_mixer_to_smc()


def main(argv=None):
    p = argparse.ArgumentParser(description="rct -> mixer_cycle_data + SMC API")
    p.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    args, _ = p.parse_known_args(argv)

    if args.once:
        run_cycle(); return

    while True:
        try:
            run_cycle()
        except Exception as exc:
            log(f"Cycle failed: {exc}")
        log(f"Sleeping {int(POLL_INTERVAL/60)} mins...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
