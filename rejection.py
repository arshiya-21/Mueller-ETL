"""
Mueller scrap-to-SandMan rejection ETL.

Every 10 min:
  1. Read production_data + the new scrap_data rows (smtpid > last processed)
     from mueller_data via SSH tunnel.
  2. Repair part numbers against components master.
  3. Match each scrap booking to the nearest pour on or before its date.
  4. Append one row per (DateScrapped, CastDate, CastingNumber) into
     scrap_production_match, with a column per defect code.
  5. Advance the smtpid marker in rejection_upload_state.

Secrets (DB_PASSWORD, SSH_PRIVATE_KEY, SSH_KEY_PASSPHRASE) can be set as env
vars for Railway (same pattern as main.py / prepared_sand.py).
"""

import atexit
import bisect
import datetime as dt
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder


# =========================================================================
# Logging — one file per day in ./logs/, keep latest 3.
# =========================================================================

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger = logging.getLogger("mueller_rejection")
logger.setLevel(logging.INFO)
_logger_date = None


def setup_logger():
    global _logger_date
    today = dt.date.today()
    if _logger_date == today and logger.handlers:
        return
    files = sorted(LOG_DIR.glob("rejection_*.log"),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    for f in files[3:]:
        try:
            f.unlink()
        except OSError:
            pass
    for h in list(logger.handlers):
        logger.removeHandler(h); h.close()
    fname = f"rejection_{today.day}_{today.month}_{today.year % 100}.log"
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(LOG_DIR / fname, encoding="utf-8")
    fh.setFormatter(fmt); logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt); logger.addHandler(sh)
    _logger_date = today


setup_logger()
log = logger.info


# =========================================================================
# Config (+ Railway env-var overrides for secrets)
# =========================================================================

BASE_DIR = Path(__file__).resolve().parent
_cfg_path = BASE_DIR / "rejection_config.json"
if not _cfg_path.exists():
    _cfg_path = BASE_DIR / "rejection_config.example.json"
CFG = json.loads(_cfg_path.read_text("utf-8"))

CFG["database"]["password"] = os.getenv("DB_PASSWORD", CFG["database"]["password"])

if os.environ.get("SSH_PRIVATE_KEY"):
    _key_path = os.path.join(tempfile.gettempdir(), "mueller_ssh_key.pem")
    with open(_key_path, "w") as _kf:
        _kf.write(os.environ["SSH_PRIVATE_KEY"])
    try:
        os.chmod(_key_path, 0o600)
    except OSError:
        pass
    CFG["ssh_tunnel"]["ssh_private_key_path"] = _key_path

if os.environ.get("SSH_KEY_PASSPHRASE"):
    CFG["ssh_tunnel"]["ssh_private_key_password"] = os.environ["SSH_KEY_PASSPHRASE"]

if not CFG["ssh_tunnel"].get("ssh_private_key_path"):
    raise SystemExit(
        "SSH key missing: set SSH_PRIVATE_KEY env var (full key contents, "
        "including BEGIN/END lines) in Railway Variables, then redeploy."
    )

DB, SSH = CFG["database"], CFG["ssh_tunnel"]
POLL_INTERVAL = int(CFG.get("poll_interval_seconds", 600))
FULL_RELOAD = bool(CFG.get("full_reload", False))

PRODUCTION_TABLE = CFG["tables"]["production"]
SCRAP_TABLE = CFG["tables"]["scrap"]
STATE_TABLE = CFG["tables"]["state"]
OUTPUT_TABLE = CFG["tables"]["output"]
COMPONENT_TABLE = CFG["tables"]["component_master"]

SRC = CFG["source_columns"]
DEFECT_NAMES = CFG["defect_names"]

CAVITIES_WHEN_MISSING = 0
APPLY_LBS_TO_KG = False
FUZZY_CUTOFF = 88
PART_RE = re.compile(r"^\d{6}-A[A-Z]?$")

MAX_LOOKBACK_DAYS = 90
LOOKBACK_FLAG_DAYS = 14
DEDUCT_AS_MATCHED = True

COLS = ["Production_Date", "Production_QTY", "Gap_Days", "Match_Type",
        "Castings_Available", "Shortfall", "Dates_Used"]


# =========================================================================
# SSH tunnel + engine (self-healing)
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
# 0. Read
# =========================================================================

def _select(table, columns):
    return ("SELECT " + ", ".join(f"{src} AS {name}" for name, src in columns.items())
            + f" FROM {table}")


def read_tables():
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} ("
            f"id INT NOT NULL PRIMARY KEY, "
            f"last_smtpid BIGINT NOT NULL DEFAULT 0, "
            f"updated_on DATETIME NULL)"))
        row = conn.execute(text(
            f"SELECT last_smtpid FROM {STATE_TABLE} WHERE id = 1")).fetchone()
    last = 0 if FULL_RELOAD else (int(row[0]) if row else 0)
    if FULL_RELOAD:
        log("FULL_RELOAD ON: reading all scrap and reloading output table")

    with engine.connect() as conn:
        sid = SRC["scrap"]["smtpid"]
        prod = pd.read_sql(text(_select(PRODUCTION_TABLE, SRC["production"])), conn)
        scrap = pd.read_sql(text(
            _select(SCRAP_TABLE, SRC["scrap"]) + f" WHERE {sid} > :last ORDER BY {sid}"),
            conn, params={"last": last})
    return prod, scrap, last


def prepare_production(prod):
    df = pd.DataFrame({
        "Foundry_Date": pd.to_datetime(prod["production_date"], errors="coerce").dt.normalize(),
        "Shift": prod["shift"],
        "Line": prod["line"],
        "PartNumber": prod["component_id"],
        "QTY": pd.to_numeric(prod["qty"], errors="coerce").fillna(0),
    })
    bad = df["Foundry_Date"].isna() | df["PartNumber"].isna()
    if bad.any():
        log(f"Warning: {int(bad.sum())} production rows had no date/component - dropped")
        df = df[~bad]
    return df.reset_index(drop=True)


# =========================================================================
# 1. Part number repair
# =========================================================================

def basic_clean(part):
    p = re.sub(r"\s+", "", str(part).upper().strip())
    p = p.replace("=", "-").replace("_", "-").replace(".", "-")
    p = re.sub(r"-{2,}", "-", p).strip("-")
    m = re.match(r"^(\d{6})(A[A-Z]?)$", p)
    return f"{m.group(1)}-{m.group(2)}" if m else p


def strip_stray_letters(part):
    head, _, tail = part.partition("-")
    head = re.sub(r"[^0-9]", "", head)
    return f"{head}-{tail}" if tail else head


def repair_parts(raw_parts, master):
    out = {}
    for raw in raw_parts:
        c = basic_clean(raw)
        if c in master:
            out[raw] = (c, "exact" if str(raw) == c else "case/separator fix", 100.0)
            continue
        q = strip_stray_letters(c)
        if q in master:
            out[raw] = (q, "stray letter removed", 100.0)
            continue
        cand = process.extractOne(q or c, master, scorer=fuzz.ratio)
        if cand and cand[1] >= FUZZY_CUTOFF:
            out[raw] = (cand[0], f"fuzzy match ({cand[1]:.0f}%)", cand[1])
        else:
            out[raw] = (str(raw).strip(), "UNMATCHED - review", cand[1] if cand else 0.0)
    return out


# =========================================================================
# 2 + 3. Load & aggregate
# =========================================================================

def load_scrap(df):
    df = df.copy()
    df["Scrap_Date"] = pd.to_datetime(df["entered"], errors="coerce").dt.normalize()
    df["PartNumber"] = df["number"].astype(str).str.strip().str.upper()
    df["Reason_Code"] = df["defect"].astype(str).str.strip().str.upper()
    df["QTY"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)

    bad_date = df["Scrap_Date"].isna()
    if bad_date.any():
        log(f"Warning: {int(bad_date.sum())} scrap rows had no usable entered date - dropped")
        df = df[~bad_date]

    unknown = sorted(set(df["Reason_Code"]) - set(DEFECT_NAMES))
    if unknown:
        log(f"Warning: reason codes not in defect_names, booked as OT: {unknown}")
        df.loc[df["Reason_Code"].isin(unknown), "Reason_Code"] = "OT"

    wide = (df.pivot_table(index=["Scrap_Date", "PartNumber"], columns="Reason_Code",
                           values="QTY", aggfunc="sum", fill_value=0).reset_index())
    wide.columns.name = None

    codes = [c for c in DEFECT_NAMES if c in wide.columns]
    wide[codes] = wide[codes].astype(int)
    wide["Scrap_QTY"] = wide[codes].sum(axis=1)
    return wide.sort_values(["PartNumber", "Scrap_Date"]).reset_index(drop=True), codes


def load_production(master_seed, summary):
    df = summary.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df["Foundry_Date"] = pd.to_datetime(df["Foundry_Date"]).dt.normalize()
    df["QTY"] = pd.to_numeric(df["QTY"], errors="coerce").fillna(0)
    df["PartNumber_Raw"] = df["PartNumber"].astype(str).str.strip()

    master = sorted(set(master_seed) |
                    {v for v in df["PartNumber_Raw"].unique() if PART_RE.match(v.upper())})
    fixes = repair_parts(sorted(df["PartNumber_Raw"].unique()), master)
    df["PartNumber"] = df["PartNumber_Raw"].map(lambda x: fixes[x][0])

    agg = (df.groupby(["Foundry_Date", "PartNumber"], as_index=False)["QTY"].sum()
             .rename(columns={"QTY": "Production_QTY"}))
    return agg


def load_component_master():
    with engine.connect() as conn:
        cm = pd.read_sql(text(_select(COMPONENT_TABLE, SRC["component_master"]) + " ORDER BY pkey"), conn)
    cm = cm[["PartNumber", "Weight", "Cavities"]].dropna(subset=["PartNumber"])
    cm["PartNumber"] = cm["PartNumber"].astype(str).str.strip().str.upper()
    cm["Cavities"] = pd.to_numeric(cm["Cavities"], errors="coerce")
    cm["Weight"] = pd.to_numeric(cm["Weight"], errors="coerce")
    if APPLY_LBS_TO_KG:
        cm["Weight"] = cm["Weight"] * 0.453592
    cm = cm.drop_duplicates("PartNumber", keep="last")
    return dict(zip(cm["PartNumber"], cm["Cavities"].fillna(CAVITIES_WHEN_MISSING)))


# =========================================================================
# 4. Match each scrap booking to its nearest pour
# =========================================================================

def _share_out(total, caps):
    out, left = {k: 0 for k in caps}, int(round(total))
    while left > 0:
        live = {k: caps[k] - out[k] for k in caps if caps[k] - out[k] > 0}
        if not live:
            break
        if left >= sum(live.values()):
            for k, v in live.items():
                out[k] += v
            break
        tw = sum(live.values())
        raw = {k: left * live[k] / tw for k in live}
        give = {k: min(int(raw[k]), live[k]) for k in live}
        rem = left - sum(give.values())
        for k in sorted(live, key=lambda x: (-(raw[x] - int(raw[x])), str(x))):
            if rem <= 0:
                break
            if give[k] < live[k]:
                give[k] += 1
                rem -= 1
        if not any(give.values()):
            break
        for k, q in give.items():
            out[k] += q
            left -= q
    return {k: v for k, v in out.items() if v > 0}


def match_nearest(scrap, prod, cav):
    qty_of = {p: dict(zip(g["Foundry_Date"], g["Production_QTY"]))
              for p, g in prod.groupby("PartNumber")}
    dates_of = {p: sorted(g["Foundry_Date"]) for p, g in prod.groupby("PartNumber")}

    left = {p: {d: qty_of[p][d] * cav.get(p, CAVITIES_WHEN_MISSING) for d in dates_of[p]}
            for p in dates_of}

    result, alloc = {}, []
    for idx in scrap.sort_values(["PartNumber", "Scrap_Date"]).index:
        part = scrap.at[idx, "PartNumber"]
        sdate = scrap.at[idx, "Scrap_Date"]
        need = int(scrap.at[idx, "Scrap_QTY"])
        dates = dates_of.get(part, [])

        i = bisect.bisect_right(dates, sdate) - 1
        if i < 0:
            result[idx] = (sdate, 0, np.nan, "No Pour On Or Before Scrap Date", 0, need, 0)
            continue

        window = sorted([d for d in dates[:i + 1]
                         if MAX_LOOKBACK_DAYS is None
                         or (sdate - d).days <= MAX_LOOKBACK_DAYS], reverse=True)
        if not window:
            gap0 = (sdate - dates[i]).days
            result[idx] = (sdate, 0, np.nan,
                           f"No Pour Within {MAX_LOOKBACK_DAYS} Days (nearest is {gap0} days back)",
                           0, need, 0)
            continue

        usable = [d for d in window if left[part][d] > 0]
        if not usable:
            result[idx] = (sdate, 0, np.nan, "No Pour With Production Left In Reach", 0, need, 0)
            continue

        big_enough = [d for d in usable if left[part][d] >= need]
        taken = {}
        if big_enough:
            d = big_enough[0]
            taken[d] = need
            left[part][d] -= need
        else:
            remaining = need
            for d in usable:
                if remaining <= 0:
                    break
                take = int(min(left[part][d], remaining))
                if take <= 0:
                    continue
                taken[d] = take
                left[part][d] -= take
                remaining -= take

        placed = sum(taken.values())
        shortfall = need - placed
        if shortfall > 0:
            taken[sdate] = taken.get(sdate, 0) + shortfall

        for d, q in taken.items():
            alloc.append({"Scrap_Date": sdate, "PartNumber": part, "Production_Date": d,
                          "Allocated_QTY": q, "Gap_Days": (sdate - d).days})

        d = max(taken, key=lambda x: (taken[x], x))
        gap = (sdate - d).days
        label = "Same Day" if gap == 0 else f"Lookback {gap} Day(s)"
        if big_enough and d != usable[0]:
            label += f" [moved back past {usable.index(d)} smaller pour(s)]"
        if len(taken) > 1:
            label += f" [excess carried back over {len(taken)} dates]"
        if shortfall:
            label += f" [{shortfall} left on the scrap date]"

        result[idx] = (d, qty_of[part].get(d, 0), gap, label,
                       int(qty_of[part].get(d, 0) * cav.get(part, CAVITIES_WHEN_MISSING)),
                       shortfall, len(taken))

    res = scrap.join(pd.DataFrame([result[i] for i in scrap.index],
                                  columns=COLS, index=scrap.index))
    lead = ["Scrap_Date", "PartNumber", "Scrap_QTY", "Production_Date", "Gap_Days",
            "Match_Type", "Production_QTY", "Castings_Available", "Shortfall", "Dates_Used"]
    return (res[lead + [c for c in res.columns if c not in lead]],
            pd.DataFrame(alloc))


def codes_by_date(matched, allocation, codes):
    spread = {}
    for _, r in allocation.iterrows():
        spread.setdefault((r["Scrap_Date"], r["PartNumber"]), {})[r["Production_Date"]] = int(r["Allocated_QTY"])

    rows = []
    for _, sr in matched.iterrows():
        code_qty = {c: int(sr[c]) for c in codes if sr[c] and sr[c] > 0}
        if not code_qty:
            continue
        dates = spread.get((sr["Scrap_Date"], sr["PartNumber"]),
                           {sr["Production_Date"]: int(sr["Scrap_QTY"])})
        if len(dates) == 1:
            d = next(iter(dates))
            for code, q in code_qty.items():
                rows.append({"Scrap_Date": sr["Scrap_Date"], "Production_Date": d,
                             "PartNumber": sr["PartNumber"], "Code": code, "QTY": q})
            continue
        caps = dict(dates)
        for code in sorted(code_qty, key=lambda c: -code_qty[c]):
            for d, q in _share_out(code_qty[code], caps).items():
                caps[d] -= q
                rows.append({"Scrap_Date": sr["Scrap_Date"], "Production_Date": d,
                             "PartNumber": sr["PartNumber"], "Code": code, "QTY": q})
    return (pd.DataFrame(rows)
            .groupby(["Scrap_Date", "Production_Date", "PartNumber", "Code"], as_index=False)["QTY"]
            .sum().astype({"QTY": int}))


# =========================================================================
# 5. Result table
# =========================================================================

def build_result(by_date):
    wide = (by_date.pivot_table(index=["Scrap_Date", "Production_Date", "PartNumber"],
                                columns="Code", values="QTY", aggfunc="sum", fill_value=0)
            .reindex(columns=list(DEFECT_NAMES), fill_value=0)
            .reset_index())
    wide.columns.name = None
    wide = wide.rename(columns={"Scrap_Date": "DateScrapped", "Production_Date": "CastDate",
                                "PartNumber": "CastingNumber", **DEFECT_NAMES})
    wide["DateScrapped"] = pd.to_datetime(wide["DateScrapped"]).dt.date
    wide["CastDate"] = pd.to_datetime(wide["CastDate"]).dt.date
    for name in DEFECT_NAMES.values():
        wide[name] = wide[name].astype(int)
    return wide.sort_values(["DateScrapped", "CastDate", "CastingNumber"]).reset_index(drop=True)


def save_results(result, last_smtpid):
    defect_cols = ", ".join(f"`{name}` INT NOT NULL DEFAULT 0" for name in DEFECT_NAMES.values())
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} ("
            f"`DateScrapped` DATE NOT NULL, `CastDate` DATE NOT NULL, "
            f"`CastingNumber` VARCHAR(50) NOT NULL, {defect_cols})"))
        if FULL_RELOAD:
            conn.execute(text(f"DELETE FROM {OUTPUT_TABLE}"))
        existing = [r[0] for r in conn.execute(text(f"SHOW COLUMNS FROM {OUTPUT_TABLE}"))]
        wanted_lower = {w.lower() for w in ["DateScrapped", "CastDate", "CastingNumber", *DEFECT_NAMES.values()]}
        have = {c.lower() for c in existing}
        for col in existing:
            if col.lower() not in wanted_lower:
                log(f"Dropping column not in config: {col}")
                conn.execute(text(f"ALTER TABLE {OUTPUT_TABLE} DROP COLUMN `{col}`"))
        for name in DEFECT_NAMES.values():
            if name.lower() not in have:
                log(f"Adding missing column: {name}")
                conn.execute(text(f"ALTER TABLE {OUTPUT_TABLE} ADD COLUMN `{name}` INT NOT NULL DEFAULT 0"))
        result.to_sql(OUTPUT_TABLE, conn, if_exists="append", index=False,
                      method="multi", chunksize=500)
        conn.execute(text(
            f"INSERT INTO {STATE_TABLE} (id, last_smtpid, updated_on) "
            f"VALUES (1, :v, NOW()) "
            f"ON DUPLICATE KEY UPDATE "
            + ("last_smtpid = VALUES(last_smtpid), " if FULL_RELOAD else
               "last_smtpid = GREATEST(last_smtpid, VALUES(last_smtpid)), ") +
            f"updated_on = NOW()"), {"v": int(last_smtpid)})


# =========================================================================
# Cycle + loop
# =========================================================================

def run_cycle():
    setup_logger()
    ensure_tunnel()

    prod_raw, scrap_raw, last_smtpid = read_tables()
    log(f"Cursor smtpid: {last_smtpid} | production rows: {len(prod_raw)} | new scrap: {len(scrap_raw)}")
    if scrap_raw.empty:
        log("No new scrap — skipping cycle.")
        return

    summary = prepare_production(prod_raw)
    scrap_agg, CODES = load_scrap(scrap_raw)
    prod_agg = load_production(scrap_agg["PartNumber"].unique(), summary)
    CAV = load_component_master()

    matched, allocation = match_nearest(scrap_agg, prod_agg, CAV)
    by_date = codes_by_date(matched, allocation, CODES)
    result = build_result(by_date)

    new_last = int(scrap_raw["smtpid"].max())
    save_results(result, new_last)
    log(f"Stored {len(result)} row(s) in {OUTPUT_TABLE}; smtpid cursor -> {new_last}")


def main():
    while True:
        try:
            run_cycle()
        except Exception as exc:
            msg = str(exc).split("\n")[0][:300]
            log(f"Cycle failed: {msg}")
        log(f"Sleeping {int(POLL_INTERVAL / 60)} mins...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
