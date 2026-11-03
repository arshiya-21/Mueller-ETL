"""
Mueller production booking ETL.

Polls production_query.sql every 10 min (matches AIW/production_booking.py's
pattern: no local cursor — the server deduplicates on (date, shift, line,
componentId, startTime, endTime) when it writes ConsumptionBooking rows).

Reads via SSH tunnel to the Mueller RDS, maps run_summary rows to the shape
/upload/save_consumption_bookings expects (see sandman-v2-mod's
ConsumptionBooking.java), and POSTs them in batches.

Secrets (DB password, API password, SSH key) can be overridden via env vars
DB_PASSWORD / API_PASSWORD / SSH_PRIVATE_KEY (Railway-friendly, same as main.py).
"""

import atexit
import datetime as dt
import json
import logging
import os
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote_plus

import requests
import urllib3
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =========================================================================
# Logging — one file per day in ./logs/, keep latest 3 files.
# =========================================================================

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger = logging.getLogger("mueller_production")
logger.setLevel(logging.INFO)
_logger_date = None


def setup_logger():
    global _logger_date
    today = dt.date.today()
    if _logger_date == today and logger.handlers:
        return

    files = sorted(LOG_DIR.glob("production_*.log"),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    for f in files[3:]:
        try:
            f.unlink()
        except OSError:
            pass

    for h in list(logger.handlers):
        logger.removeHandler(h); h.close()

    fname = f"production_{today.day}_{today.month}_{today.year % 100}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_DIR / fname, encoding="utf-8")
    fh.setFormatter(fmt); logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt); logger.addHandler(sh)
    _logger_date = today


setup_logger()
log = logger.info


# =========================================================================
# Config
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
_cfg_path = BASE_DIR / "production_config.json"
if not _cfg_path.exists():
    _cfg_path = BASE_DIR / "production_config.example.json"
CFG = json.loads(_cfg_path.read_text("utf-8"))

CFG["database"]["password"] = os.getenv("DB_PASSWORD", CFG["database"]["password"])
CFG["api"]["j_password"] = os.getenv("API_PASSWORD", CFG["api"]["j_password"])

if os.environ.get("SSH_PRIVATE_KEY"):
    _key_path = os.path.join(tempfile.gettempdir(), "mueller_ssh_key.pem")
    with open(_key_path, "w") as _kf:
        _kf.write(os.environ["SSH_PRIVATE_KEY"])
    try:
        os.chmod(_key_path, 0o600)
    except OSError:
        pass
    CFG["ssh_tunnel"]["ssh_private_key_path"] = _key_path

DB, SSH, API = CFG["database"], CFG["ssh_tunnel"], CFG["api"]
POLL_INTERVAL = int(CFG.get("poll_interval_seconds", 600))
FOUNDRY_LINE_ID = int(CFG["foundry_line_id"])
TEMP_LINE_MAP = {k: int(v) for k, v in CFG["temp_line_map"].items()}
QUERY_FILE = CFG.get("query_file", "production_query.sql")
PRODUCTION_TABLE = CFG.get("production_table", "production_data")


# =========================================================================
# SSH tunnel + DB engine
# =========================================================================

def _make_tunnel():
    return SSHTunnelForwarder(
        (SSH["ssh_host"], SSH.get("ssh_port", 22)),
        ssh_username=SSH["ssh_username"],
        ssh_pkey=SSH["ssh_private_key_path"],
        ssh_private_key_password=SSH.get("ssh_private_key_password") or None,
        remote_bind_address=(DB["host"], int(DB["port"])),
        # Keepalive every 30s during long queries. (Doesn't help during our
        # own 10-min time.sleep — see ensure_tunnel() below.)
        set_keepalive=30,
    )


tunnel = _make_tunnel()
tunnel.start()
atexit.register(tunnel.stop)


def ensure_tunnel():
    """Called at the start of every cycle. The AWS bastion drops the SSH
    session during our 10-min idle sleep (set_keepalive stops firing when
    the main thread is sleeping or the paramiko transport closes). If the
    tunnel is dead, tear it down and start a fresh one — and point the
    engine at the new local bind port."""
    global tunnel, engine
    try:
        if tunnel.is_active and tunnel.is_alive:
            return
    except Exception:
        pass
    log("SSH tunnel is down; reopening...")
    try:
        tunnel.stop()
    except Exception:
        pass
    tunnel = _make_tunnel()
    tunnel.start()
    engine.dispose()                   # kill stale pooled connections
    engine = _make_engine()

def _make_engine():
    return create_engine(
        f"mysql+pymysql://{quote_plus(DB['user'])}:{quote_plus(DB['password'])}"
        f"@127.0.0.1:{tunnel.local_bind_port}/{DB['database_name']}",
        pool_pre_ping=True,
        # 20-min client-side read/write timeout so a slow query doesn't
        # trigger "Lost connection to MySQL server during query".
        connect_args={"read_timeout": 1200, "write_timeout": 1200, "connect_timeout": 60},
    )


engine = _make_engine()


# =========================================================================
# Query loading — split on `;` so the leading `SET @...` statements run on
# the same connection as the main SELECT (same MySQL session).
# `--` line comments are stripped from every line BEFORE splitting; otherwise
# inline comments like `SET @x = 1;  -- note` leave the next chunk starting
# with `-- note\nSET @y = ...` and simple start-with-"--" filters wipe it.
# =========================================================================

def _load_statements(path):
    lines = []
    for ln in path.read_text("utf-8").splitlines():
        idx = ln.find("--")
        if idx >= 0:
            ln = ln[:idx]
        lines.append(ln)
    cleaned = "\n".join(lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


_STATEMENTS = _load_statements(BASE_DIR / QUERY_FILE)


def fetch_production_rows():
    """Run production_query.sql — SET @vars + main SELECT on the same session.
    Extends session wait/net timeouts to 20 min so a slow backfill query
    survives MySQL's default 28800s / 30s idle & read ceilings."""
    with engine.connect() as conn:
        conn.execute(text("SET SESSION wait_timeout = 1200"))
        conn.execute(text("SET SESSION interactive_timeout = 1200"))
        conn.execute(text("SET SESSION net_read_timeout = 1200"))
        conn.execute(text("SET SESSION net_write_timeout = 1200"))
        for stmt in _STATEMENTS[:-1]:
            conn.execute(text(stmt))                 # SET @lag_min = 11, etc.
        res = conn.execute(text(_STATEMENTS[-1]))     # main SELECT
        return res.mappings().all()


# =========================================================================
# Stage A — upsert query rows into local production_data table.
# Unique key = (production_date, shift, line, component_id, start_time) so
# re-running the SQL every 10 min UPDATEs runs that grew (more boxes, new
# additive averages) and INSERTs any brand-new runs. Table auto-created.
# =========================================================================

PRODUCTION_DDL = f"""
CREATE TABLE IF NOT EXISTS {PRODUCTION_TABLE} (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    production_date DATE          NOT NULL,
    shift           TINYINT       NOT NULL,
    component_id    VARCHAR(8)    NOT NULL,
    line            VARCHAR(10)   NOT NULL,
    start_time      TIME          NOT NULL,
    end_time        TIME          NULL,
    boxes_poured    INT           NULL,
    batches         INT           NULL,
    total_mould     DOUBLE        NULL,
    unpoured_mould  DOUBLE        NULL,
    weight_bento_kg DECIMAL(6,2)  NULL,
    poids_sn        DECIMAL(6,2)  NULL,
    total_water_l   DECIMAL(6,2)  NULL,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_run (production_date, shift, line, component_id, start_time)
) ENGINE=InnoDB
"""

PRODUCTION_UPSERT = f"""
INSERT INTO {PRODUCTION_TABLE}
    (production_date, shift, component_id, line, start_time, end_time,
     boxes_poured, batches, total_mould, unpoured_mould,
     weight_bento_kg, poids_sn, total_water_l)
VALUES
    (:production_date, :shift, :component_id, :line, :start_time, :end_time,
     :boxes_poured, :batches, :total_mould, :unpoured_mould,
     :weight_bento_kg, :poids_sn, :total_water_l)
ON DUPLICATE KEY UPDATE
    end_time         = VALUES(end_time),
    boxes_poured     = VALUES(boxes_poured),
    batches          = VALUES(batches),
    total_mould      = VALUES(total_mould),
    unpoured_mould   = VALUES(unpoured_mould),
    weight_bento_kg  = VALUES(weight_bento_kg),
    poids_sn         = VALUES(poids_sn),
    total_water_l    = VALUES(total_water_l)
"""


def _init_production_table():
    with engine.begin() as conn:
        conn.execute(text(PRODUCTION_DDL))


_init_production_table()


def upsert_production(rows):
    if not rows:
        return 0
    params = []
    for r in rows:
        boxes = int(r["No of Boxes Poured"] or 0)
        params.append({
            "production_date": r["Date"],
            "shift":           int(r["Shift"]),
            "component_id":    str(r["Component ID"]),
            "line":            r["Line"],
            "start_time":      _fmt_time(r["Start Time"]),
            "end_time":        _fmt_time(r["End Time"]),
            "boxes_poured":    boxes,
            "batches":         int(r["No of Batches"] or 0),
            # total_mould mirrors boxes_poured; unpoured_mould always 0 for Mueller.
            "total_mould":     float(boxes),
            "unpoured_mould":  0.0,
            "weight_bento_kg": _num(r["weight_bento_kg"]),
            "poids_sn":        _num(r["poids_sn"]),
            "total_water_l":   _num(r["total_water_l"]),
        })
    with engine.begin() as conn:
        conn.execute(text(PRODUCTION_UPSERT), params)
    return len(params)


# Stage B — read back from the local table. This is what we send to the API,
# NOT the raw query result. That way "whatever is in production_data IS what
# is in cloud" is an invariant the code enforces: a row cannot reach the API
# without first being stored locally.

PRODUCTION_SELECT = text(f"""
    SELECT production_date, shift, component_id, line, start_time, end_time,
           boxes_poured, batches, total_mould, unpoured_mould,
           weight_bento_kg, poids_sn, total_water_l
    FROM {PRODUCTION_TABLE}
    ORDER BY production_date, shift, line, start_time
""")


def read_production_rows():
    with engine.connect() as conn:
        return conn.execute(PRODUCTION_SELECT).mappings().all()




# =========================================================================
# Row -> API payload (ConsumptionBooking shape)
# =========================================================================

def _fmt_time(v):
    if isinstance(v, dt.timedelta):
        s = int(v.total_seconds())
        return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"
    return v.strftime("%H:%M:%S") if hasattr(v, "strftime") else str(v)


def _fmt_date(v):
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _num(v):
    """MySQL ROUND(AVG(...)) returns decimal.Decimal; json can't serialize that.
    Cast all numerics (Decimal, int, float) to plain float for JSON."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v) if isinstance(v, (int, float)) else v


def build_payload(rows):
    """production_data rows -> ConsumptionBooking dicts matching the Java bean's
    camelCase getters. foundryLineId is the fixed Mueller line pkey; tempLine
    is the per-Disa integer from config.temp_line_map."""
    payload = []
    for r in rows:
        boxes = _num(r["boxes_poured"]) or 0.0
        entry = {
            "date":              _fmt_date(r["production_date"]),
            "shift":             str(r["shift"]),
            "startTime":         _fmt_time(r["start_time"]),
            "endTime":           _fmt_time(r["end_time"]),
            "componentId":       str(r["component_id"]),
            "foundryLineId":     FOUNDRY_LINE_ID,
            "tempLine":          TEMP_LINE_MAP.get(r["line"]),
            "noOfBoxesPoured":   boxes,
            "noOfBatches":       int(r["batches"] or 0),
            "totalMould":        _num(r["total_mould"]) or boxes,
            "unpouredMould":     _num(r["unpoured_mould"]) or 0.0,
            "bentoniteSetPoint": _num(r["weight_bento_kg"]),
            "newSandSetPoint":   _num(r["poids_sn"]),
            "waterSetPoint":     _num(r["total_water_l"]),
        }
        entry = {k: v for k, v in entry.items() if v is not None}
        payload.append(entry)
    return payload


# =========================================================================
# API session (same login flow as main.py / AIW / Crescent)
# =========================================================================

_session = requests.Session()
_session.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36")
_jsessionid = None


def api_login():
    global _jsessionid
    r = _session.post(API["base_url"] + API["login_url"],
                      data={"j_username": API["j_username"], "j_password": API["j_password"]},
                      verify=False, timeout=30, allow_redirects=True)
    _jsessionid = _session.cookies.get("JSESSIONID")
    ok = r.status_code == 200 and bool(_jsessionid)
    log(f"API login: HTTP {r.status_code} | final={r.url} | authenticated={ok}")
    return ok


def _post(payload):
    headers = {"Content-Type": "application/json"}
    xsrf = _session.cookies.get("XSRF-TOKEN")
    if xsrf:
        headers["X-CSRF-TOKEN"] = xsrf
    return _session.post(
        API["base_url"] + API["upload_url"], json=payload, verify=False,
        timeout=120, headers=headers, allow_redirects=False,
    )


def _is_authenticated_response(r):
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


def upload_bookings(payload):
    size = int(API.get("batch_size", 100))
    if payload:
        log(f"First record being sent: {json.dumps(payload[0])}")
    for i in range(0, len(payload), size):
        chunk = payload[i:i + size]
        if not post_batch(chunk):
            log(f"Batch {i // size + 1} failed; stopping.")
            return False
        log(f"Batch {i // size + 1}: {len(chunk)} row(s) uploaded.")
    return True


# =========================================================================
# Cycle + loop
# =========================================================================

def run_cycle():
    setup_logger()
    ensure_tunnel()

    # Stage A: pull from pouring_data via the SQL and upsert into
    # production_data (local source of truth).
    query_rows = fetch_production_rows()
    log(f"Query returned {len(query_rows)} row(s)")
    upsert_production(query_rows)
    log(f"Upserted {len(query_rows)} row(s) into {PRODUCTION_TABLE}")

    # Stage B: read EVERYTHING in production_data and push to the API.
    # Reading from local (not from query_rows) enforces strict parity —
    # whatever COUNT is in local is exactly the COUNT sent to cloud.
    db_rows = read_production_rows()
    payload = build_payload(db_rows)
    log(f"Sending {len(payload)} booking record(s) from {PRODUCTION_TABLE}")
    upload_bookings(payload)


def main():
    while True:
        try:
            run_cycle()
        except Exception as exc:
            log(f"Cycle failed: {exc}")
        log(f"Sleeping {int(POLL_INTERVAL / 60)} mins...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
