"""
Mueller prepared-sand (sand_lab) ETL.

Polls prepared_sand_query.sql every 10 min (last 7 days rolling window),
upserts rows into local `prepared_sand` audit table, and POSTs only the
not-yet-sent rows to /upload/savePreparesand.

Why the sent_at flag matters: unlike /upload/save_consumption_bookings, the
savePreparesand endpoint does a straight INSERT in genericDao.save, with
no server-side dedup. If we resent the same 7-day window every cycle, the
server would accumulate duplicate rows. The sent_at flag ensures each
physical sand_lab reading is POSTed exactly once.

Secrets (DB_PASSWORD, API_PASSWORD, SSH_PRIVATE_KEY) can be overridden via
env vars for Railway deployment (same pattern as main.py and production_booking.py).
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
logger = logging.getLogger("mueller_prepared_sand")
logger.setLevel(logging.INFO)
_logger_date = None


def setup_logger():
    global _logger_date
    today = dt.date.today()
    if _logger_date == today and logger.handlers:
        return

    files = sorted(LOG_DIR.glob("prepared_sand_*.log"),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    for f in files[3:]:
        try:
            f.unlink()
        except OSError:
            pass

    for h in list(logger.handlers):
        logger.removeHandler(h); h.close()

    fname = f"prepared_sand_{today.day}_{today.month}_{today.year % 100}.log"
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
_cfg_path = BASE_DIR / "prepared_sand_config.json"
if not _cfg_path.exists():
    _cfg_path = BASE_DIR / "prepared_sand_config.example.json"
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

if not CFG["ssh_tunnel"].get("ssh_private_key_path"):
    raise SystemExit(
        "SSH key missing: set SSH_PRIVATE_KEY env var (full key contents, "
        "including BEGIN/END lines) in Railway Variables, then redeploy."
    )

DB, SSH, API = CFG["database"], CFG["ssh_tunnel"], CFG["api"]
POLL_INTERVAL = int(CFG.get("poll_interval_seconds", 600))
QUERY_FILE = CFG.get("query_file", "prepared_sand_query.sql")
PREPARED_SAND_TABLE = CFG.get("prepared_sand_table", "prepared_sand")
STATE_TABLE = CFG.get("sync_state_table", "mueller_sync_state")
CURSOR_SOURCE = CFG.get("cursor_source_name", "sandlab")


# =========================================================================
# SSH tunnel + DB engine (self-healing on each cycle)
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
    the 10-min idle sleep."""
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
    engine.dispose()
    engine = _make_engine()


# =========================================================================
# Query loading — split on `;` so SET @start_date/@end_date run on the
# same connection as the main SELECT (same MySQL session).
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


def fetch_prepared_sand_rows(last_id):
    """SET @last_id + main SELECT on the same session. The SQL's leading
    SET @last_id = COALESCE(@last_id, 0) picks up whatever we set here, so
    the query only returns sand_lab rows past the cursor. 20-min client-
    side timeouts for safety on a slow backfill query."""
    with engine.connect() as conn:
        conn.execute(text("SET SESSION wait_timeout = 1200"))
        conn.execute(text("SET SESSION interactive_timeout = 1200"))
        conn.execute(text("SET SESSION net_read_timeout = 1200"))
        conn.execute(text("SET SESSION net_write_timeout = 1200"))
        conn.execute(text("SET @last_id = :last"), {"last": int(last_id)})
        for stmt in _STATEMENTS[:-1]:
            conn.execute(text(stmt))
        res = conn.execute(text(_STATEMENTS[-1]))
        return res.mappings().all()


# =========================================================================
# Stage A — upsert query rows into local prepared_sand audit table.
# Unique key = sand_lab_id (upstream id). Re-fetching the same row is a
# no-op UPDATE; truly new rows INSERT. Local is the audit of EVERY row
# we've seen; cursor (below) tracks what's been POSTed to the cloud.
# =========================================================================

_LOCAL_COLUMNS = [
    "sand_lab_id",   # upstream sand_lab.id — primary dedup key
    "active_clay", "ambient_temp", "compactibility", "component_id",
    "cone_jolt_test", "date", "friability_index", "gcs", "gfn_afs",
    "heat_no", "inert_fines", "loi", "moisture", "oolitics", "pH_value",
    "permeability", "relative_humidity", "shatter_index_no", "shear_strength",
    "shift", "specimen_weight", "split_strength", "temp_of_sand_after_mix",
    "temp_of_sand_before_mix", "time", "total_clay",
    "volatile_combustible_matter", "volatile_matter", "wet_tensile_strength",
    "dry_compression_strength", "quality_inspector", "temp_line",
    "inventory_detail_json",
]


PREPARED_SAND_DDL = f"""
CREATE TABLE IF NOT EXISTS {PREPARED_SAND_TABLE} (
    id                              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sand_lab_id                     BIGINT UNSIGNED NOT NULL,

    active_clay                     DOUBLE        NULL,
    ambient_temp                    DOUBLE        NULL,
    compactibility                  DOUBLE        NULL,
    component_id                    VARCHAR(32)   NULL,
    cone_jolt_test                  DOUBLE        NULL,
    `date`                          DATETIME      NOT NULL,
    friability_index                DOUBLE        NULL,
    gcs                             DOUBLE        NULL,
    gfn_afs                         DOUBLE        NULL,
    heat_no                         VARCHAR(32)   NULL,
    inert_fines                     DOUBLE        NULL,
    loi                             DOUBLE        NULL,
    moisture                        DOUBLE        NULL,
    oolitics                        DOUBLE        NULL,
    pH_value                        DOUBLE        NULL,
    permeability                    DOUBLE        NULL,
    relative_humidity               DOUBLE        NULL,
    shatter_index_no                DOUBLE        NULL,
    shear_strength                  DOUBLE        NULL,
    shift                           VARCHAR(4)    NULL,
    specimen_weight                 DOUBLE        NULL,
    split_strength                  DOUBLE        NULL,
    temp_of_sand_after_mix          DOUBLE        NULL,
    temp_of_sand_before_mix         DOUBLE        NULL,
    `time`                          TIME          NOT NULL,
    total_clay                      DOUBLE        NULL,
    volatile_combustible_matter     DOUBLE        NULL,
    volatile_matter                 DOUBLE        NULL,
    wet_tensile_strength            DOUBLE        NULL,
    dry_compression_strength        DOUBLE        NULL,
    quality_inspector               VARCHAR(64)   NULL,
    temp_line                       VARCHAR(64)   NOT NULL,
    inventory_detail_json           TEXT          NULL,

    created_at                      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_sand_lab_id (sand_lab_id)
) ENGINE=InnoDB
"""


_PLACEHOLDERS = ", ".join(f":{c}" for c in _LOCAL_COLUMNS)
_UPDATE_CLAUSE = ", ".join(
    f"`{c}` = VALUES(`{c}`)" for c in _LOCAL_COLUMNS if c != "sand_lab_id"
)
PREPARED_SAND_UPSERT = f"""
INSERT INTO {PREPARED_SAND_TABLE}
    ({", ".join(f"`{c}`" for c in _LOCAL_COLUMNS)})
VALUES
    ({_PLACEHOLDERS})
ON DUPLICATE KEY UPDATE
    {_UPDATE_CLAUSE}
"""


# =========================================================================
# Cursor — tracked in mueller_sync_state (shared with main.py's rct/mixer).
# source_table = 'sandlab', last_id = highest sand_lab.id already POSTed.
# =========================================================================

SYNC_STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    last_id INT NOT NULL DEFAULT 0,
    source_table VARCHAR(32) NOT NULL UNIQUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB
"""


def _init_schema():
    with engine.begin() as conn:
        conn.execute(text(PREPARED_SAND_DDL))
        conn.execute(text(SYNC_STATE_DDL))
        conn.execute(text(
            f"INSERT IGNORE INTO {STATE_TABLE} (source_table) VALUES (:s)"
        ), {"s": CURSOR_SOURCE})


_init_schema()


def get_cursor():
    """Take the greater of the two local cursors:
      - mueller_sync_state.last_id (survives if prepared_sand is dropped)
      - MAX(prepared_sand.sand_lab_id) (survives if mueller_sync_state is dropped)
    Only unprotected case is both wiped simultaneously, which should not happen
    unless someone drops the whole mueller_data schema."""
    with engine.connect() as conn:
        return int(conn.execute(text(f"""
            SELECT GREATEST(
                COALESCE((SELECT last_id FROM {STATE_TABLE}
                          WHERE source_table = :s), 0),
                COALESCE((SELECT MAX(sand_lab_id) FROM {PREPARED_SAND_TABLE}), 0)
            )
        """), {"s": CURSOR_SOURCE}).scalar() or 0)


def set_cursor(conn, last_id):
    conn.execute(text(
        f"UPDATE {STATE_TABLE} SET last_id = :i WHERE source_table = :s"
    ), {"i": int(last_id), "s": CURSOR_SOURCE})


def _num(v):
    """MySQL ROUND/AVG returns decimal.Decimal; cast to plain float."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v) if isinstance(v, (int, float)) else v


def _fmt_time(v):
    if isinstance(v, dt.timedelta):
        s = int(v.total_seconds())
        return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"
    return v.strftime("%H:%M:%S") if hasattr(v, "strftime") else str(v)


def _row_to_params(r):
    """SQL row (RowMapping) -> parameter dict for the upsert. All numeric
    fields pass through _num so Decimals become floats; date stays as
    datetime for MySQL; time becomes HH:MM:SS string."""
    params = {}
    for col in _LOCAL_COLUMNS:
        v = r.get(col)
        if v is None:
            params[col] = None
        elif col == "sand_lab_id":
            params[col] = int(v)
        elif col == "time":
            params[col] = _fmt_time(v)
        elif col in ("component_id", "heat_no", "shift", "quality_inspector",
                     "temp_line", "inventory_detail_json"):
            params[col] = str(v)
        elif col == "date":
            params[col] = v   # pass datetime/date straight through
        else:
            params[col] = _num(v)
    return params


def upsert_prepared_sand(rows):
    if not rows:
        return 0
    params = [_row_to_params(r) for r in rows]
    with engine.begin() as conn:
        conn.execute(text(PREPARED_SAND_UPSERT), params)
    return len(params)


# =========================================================================
# Stage B — POST to /upload/savePreparesand, advance cursor per batch.
# The SQL already filtered by s.id > @last_id, so every fetched row is new.
# =========================================================================


# snake_case (local column) -> API camelCase. Keys that don't appear in the
# source row are omitted from the payload. Matches PreparedSand.java getters.
FIELD_MAP = {
    "active_clay":                 "activeClay",
    "ambient_temp":                "ambientTemp",
    "compactibility":              "compactibility",
    "component_id":                "componentId",
    "cone_jolt_test":              "coneJoltTest",
    "friability_index":            "friabilityIndex",
    "gcs":                         "gcs",
    "gfn_afs":                     "gfnAfs",
    "heat_no":                     "heatNo",
    "inert_fines":                 "inertFines",
    "loi":                         "loi",
    "moisture":                    "moisture",
    "oolitics":                    "oolitics",
    "pH_value":                    "pHValue",
    "permeability":                "permeability",
    "relative_humidity":           "relativeHumidity",
    "shatter_index_no":            "shatterIndexNo",
    "shear_strength":              "shearStrength",
    "shift":                       "shift",
    "specimen_weight":             "specimenWeight",
    "split_strength":              "splitStrength",
    "temp_of_sand_after_mix":      "tempOfSandAfterMix",
    "temp_of_sand_before_mix":     "tempOfSandBeforeMix",
    "total_clay":                  "totalClay",
    "volatile_combustible_matter": "volatileCombustibleMatter",
    "volatile_matter":             "volatileMatter",
    "wet_tensile_strength":        "wetTensileStrength",
    "dry_compression_strength":    "dryCompressionStrength",
    "quality_inspector":           "qualityInspector",
    "temp_line":                   "tempLine",
    "inventory_detail_json":       "inventoryDetailJson",
}


def _date_to_ms(v):
    """savePreparesand's controller does Long dateStr = (Long) jsonObj.get("date")
    then new Date(dateStr), so date MUST be a numeric milliseconds-since-epoch.
    Normalize to UTC midnight of the stored date (server truncates to day anyway)."""
    if isinstance(v, dt.datetime):
        d = v.date()
    elif isinstance(v, dt.date):
        d = v
    else:
        return None
    ts = dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
    return int(ts.timestamp() * 1000)


def build_payload(rows):
    """local rows -> dicts the server expects. `date` is Long millis (quirk
    of this endpoint); `time` is HH:mm:ss string; everything else goes
    through FIELD_MAP with snake_case -> camelCase. Numeric 0 is treated as
    "no reading" and sent as JSON null (key is kept, value is None)."""
    payload = []
    for r in rows:
        entry = {
            "date": _date_to_ms(r["date"]),
            "time": _fmt_time(r["time"]),
        }
        for src, dest in FIELD_MAP.items():
            v = r.get(src)
            if v is None:
                entry[dest] = None
                continue
            if isinstance(v, Decimal):
                v = float(v)
            # Numeric 0 -> null (keep the key but send null).
            if isinstance(v, (int, float)) and v == 0:
                entry[dest] = None
            else:
                entry[dest] = v
        payload.append(entry)
    return payload


# =========================================================================
# API session (same login flow as the other scripts)
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
    log(f"Login: {'ok' if ok else 'FAILED'}")
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
    """savePreparesand returns {"Status": "Success"} or {"Status": "Error", ...}
    on HTTP 200 either way. Treat a 200 with "Status":"Error" as a failure
    so we don't mark bad rows as sent."""
    if _jsessionid is None and not api_login():
        return False
    r = _post(payload)
    if not _is_authenticated_response(r):
        log(f"Not authenticated (HTTP {r.status_code}); re-logging in and retrying.")
        if not api_login():
            return False
        r = _post(payload)

    # Parse JSON body; Status != "Success" means the server rejected it.
    try:
        parsed = r.json()
    except Exception:
        parsed = None
    status_ok = isinstance(parsed, dict) and parsed.get("Status") == "Success"
    ok = r.ok and status_ok
    if not ok:
        log(f"POST failed: HTTP {r.status_code} | {r.text[:200]}")
    return ok


def upload_prepared_sand(payload, sand_lab_ids):
    """POSTs in batches; after each successful batch, advances the sandlab
    cursor in mueller_sync_state to the max sand_lab_id in that batch. If
    a batch fails mid-run, the cursor stops advancing — next cycle picks
    up exactly from the last successful batch."""
    if not payload:
        return 0
    size = int(API.get("batch_size", 100))
    total = len(payload)
    batches = (total + size - 1) // size
    sent_total = 0
    for i in range(0, total, size):
        n = i // size + 1
        chunk_payload = payload[i:i + size]
        chunk_ids = sand_lab_ids[i:i + size]
        if not post_batch(chunk_payload):
            log(f"Batch {n}/{batches} FAILED. Cursor held at last success.")
            return sent_total
        with engine.begin() as conn:
            set_cursor(conn, max(chunk_ids))
        sent_total += len(chunk_payload)
        log(f"Batch {n}/{batches} ok ({len(chunk_payload)} rows, cursor -> {max(chunk_ids)})")
    return sent_total


# =========================================================================
# Cycle + loop
# =========================================================================

def run_cycle():
    setup_logger()
    ensure_tunnel()

    cursor = get_cursor()
    query_rows = fetch_prepared_sand_rows(cursor)
    n = len(query_rows)
    log(f"Cursor: {cursor} | sand_lab new: {n}")

    if not n:
        return

    upsert_prepared_sand(query_rows)
    log(f"Stored {n} row(s) in {PREPARED_SAND_TABLE}")

    sand_lab_ids = [int(r["sand_lab_id"]) for r in query_rows]
    payload = build_payload(query_rows)
    sent = upload_prepared_sand(payload, sand_lab_ids)
    log(f"Done — sent {sent}/{n} to API")


def main():
    while True:
        try:
            run_cycle()
        except Exception as exc:
            # SQLAlchemy's str(exc) includes the full failing SQL — truncate so the
            # log stays readable. Full stacktrace still goes to debug if needed.
            msg = str(exc).split("\n")[0][:300]
            log(f"Cycle failed: {msg}")
        log(f"Sleep {int(POLL_INTERVAL / 60)}m")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
