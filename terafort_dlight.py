#!/usr/bin/env python3
"""
================================================================================
 D-LIGHT (Dlightek / Transsion) APK GAME DATA  ->  BIGQUERY
================================================================================
 Self-authenticating, login-per-run pipeline. No token babysitting: every run
 logs in fresh, so an expired session is impossible by construction.

 AUTH CHAIN (all replicated in one requests.Session cookie jar):
   1. POST eagllwin/.../loginByEmailAndPassword   {email, password: MD5(pwd)}
                                                  -> accessToken
   2. POST .../getOrganizationAaaListByQuery       -> resolve aaaId (dynamic)
   3. POST .../switchAaaAccount        {aaaId}     -> refreshed accessToken
   4. POST .../getBusinessAccountListByQuery       -> resolve businessAccountId
   5. POST .../switchBusinessAccountById           -> refreshed accessToken
   6. POST .../getAuthCode             {accessToken}
   7. GET  dlightek/api/aha/getAhaGameToken (SESSION cookie) -> 24h JWT
   8. GET  data.ahagamecenter.com/api/adInstallPay/list (header token: JWT)

 DATA PULL:
   * Dual timezone: pulls the FULL window once per tz in TIMEZONES (UTC, UTC+5).
   * Generic pagination: read data.total, loop ceil(total/limit) pages,
     assert collected == total per timezone, else explicit-fail.
   * Defensive parsing: handles null / "" / "50.00%" / int / float / object.

 LAND -> BigQuery MERGE on (dw_date, game_package, timezone) so the two tz
 versions coexist and daily re-pulls restate cleanly (never duplicate).

 Bleed-proof rules:
   * Every API response checked: code, message, result-shape. Expired session
     ("Account not logged in!") -> explicit-fail (shouldn't happen: we log in
     fresh, but detected anyway).
   * Transient 5xx / network -> retry w/ backoff. 4xx -> never retried.
   * No row dropped silently; no metric silently defaulted.

 Required env:
   DLIGHT_EMAIL, DLIGHT_PASSWORD   credentials (password hashed at runtime)
   BQ_PROJECT
 Optional env:
   BQ_DATASET (default dlight)  BQ_TABLE (default dlight_daily)
   BQ_LOCATION (default US)
   BACKFILL_START (default 2026-01-01)   first date for the one-time backfill
   LOOKBACK_DAYS (default 30)            rolling window for normal daily runs
   FULL_BACKFILL ("1" -> from BACKFILL_START to today; else rolling lookback)
   PAGE_LIMIT (default 500)             rows per page (generic loop regardless)
   TIMEZONES (default "UTC,UTC+5")      comma list of accept-timezone values
   DRY_RUN ("1" -> pull+validate, write local NDJSON only)
================================================================================
"""
import datetime as dt
import hashlib
import json
import math
import os
import sys
import time
from zoneinfo import ZoneInfo

import requests

# ------------------------------------------------------------------ endpoints
EAG = "https://api.eagllwin.com"
LOGIN_URL   = f"{EAG}/common/authority/tmc-not-login/tmcuser/cmd/loginByEmailAndPassword"
ORG_URL     = f"{EAG}/common/authority/advertiser/authoritymember/query/getOrganizationAaaListByQuery"
SWITCH_AAA  = f"{EAG}/common/authority/tmc-not-login/authoritymember/cmd/switchAaaAccount"
BIZ_URL     = f"{EAG}/common/authority/advertiser/authoritymember/query/getBusinessAccountListByQuery"
SWITCH_BIZ  = f"{EAG}/common/authority/tmc/authoritymember/cmd/switchBusinessAccountById"
AUTHCODE_URL= f"{EAG}/common/authority/tmc-not-login/tmcuser/cmd/getAuthCode"
AHA_TOKEN_URL = "https://dev.dlightek.com/api/aha/getAhaGameToken"
LIST_URL    = "https://data.ahagamecenter.com/api/adInstallPay/list"

HTTP_TIMEOUT = 60
PKT = ZoneInfo("Asia/Karachi")

# gateway "costume" headers eagllwin requires on every call
EAG_HEADERS_BASE = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Accept-Timezone": "UTC+5",
    "Business-Type": "19",
    "Device-Type": "1",
    "Device-Model": "Netscape",
    "Endpoint-Type": "6",
    "Origin": "https://portal.lionnan.com",
    "Referer": "https://portal.lionnan.com/",
    "X-Tr-Devtype": "h5",
    "X-Tr-Region": "CN",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"),
}


def fail(msg: str) -> None:
    print(f"\n🚨 DLIGHT PIPELINE FAILED: {msg}", file=sys.stderr)
    sys.exit(1)


def env(name, default=None, required=False):
    v = os.environ.get(name, default)
    if required and not v:
        fail(f"missing required env var: {name}")
    return v


# ------------------------------------------------------------------ config
EMAIL = env("DLIGHT_EMAIL", required=True)
PASSWORD = env("DLIGHT_PASSWORD", required=True)
DRY_RUN = env("DRY_RUN", "0") == "1"
BQ_PROJECT = env("BQ_PROJECT", required=not DRY_RUN)
BQ_DATASET = env("BQ_DATASET", "dlight")
BQ_TABLE = env("BQ_TABLE", "dlight_daily")
BQ_LOCATION = env("BQ_LOCATION", "US")
BACKFILL_START = env("BACKFILL_START", "2026-01-01")
LOOKBACK_DAYS = int(env("LOOKBACK_DAYS", "30"))
FULL_BACKFILL = env("FULL_BACKFILL", "0") == "1"
PAGE_LIMIT = int(env("PAGE_LIMIT", "500"))
TIMEZONES = [t.strip() for t in env("TIMEZONES", "UTC,UTC+5").split(",") if t.strip()]


def md5_upper(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest().upper()


# ------------------------------------------------------------ HTTP helpers
def _request(session, method, url, *, headers, json_body=None, step, retries=4):
    delay = 5
    for i in range(1, retries + 1):
        try:
            resp = session.request(method, url, headers=headers, json=json_body,
                                   timeout=HTTP_TIMEOUT)
        except requests.RequestException as exc:
            if i == retries:
                fail(f"[{step}] network error after {retries} attempts: {exc}")
            print(f"⚠️  [{step}] attempt {i}/{retries} network error; retry {delay}s")
            time.sleep(delay); delay *= 2; continue
        if resp.status_code >= 500:
            if i == retries:
                fail(f"[{step}] HTTP {resp.status_code} after {retries} attempts")
            print(f"⚠️  [{step}] attempt {i}/{retries} HTTP {resp.status_code}; retry {delay}s")
            time.sleep(delay); delay *= 2; continue
        if resp.status_code >= 400:
            fail(f"[{step}] HTTP {resp.status_code}: {resp.text[:300]}")
        return resp
    fail(f"[{step}] exhausted retries")  # unreachable


def _json_ok(resp, step):
    try:
        data = resp.json()
    except ValueError:
        fail(f"[{step}] non-JSON response: {resp.text[:300]}")
    code = str(data.get("code", ""))
    result = data.get("result")
    # expired-session signature: result is a bilingual error array
    if isinstance(result, list) and any("not logged in" in str(x).lower()
                                        or "未登录" in str(x) for x in result):
        fail(f"[{step}] session not authenticated (result={result}). "
             f"This should not happen on a fresh login -- check credentials.")
    if code not in ("0", "200"):
        fail(f"[{step}] API code={code} message={data.get('message')!r}")
    return data


# ------------------------------------------------------------ auth chain
def authenticate() -> tuple[requests.Session, str]:
    """Run the full SSO chain; return (session, aha_jwt)."""
    s = requests.Session()

    # 1. login
    body = {"email": EMAIL, "password": md5_upper(PASSWORD),
            "captchaKey": "", "emailCaptcha": ""}
    data = _json_ok(_request(s, "POST", LOGIN_URL, headers=EAG_HEADERS_BASE,
                             json_body=body, step="login"), "login")
    access = _extract_access_token(data, "login")
    print("✅ login ok")

    # 2. resolve aaaId dynamically
    h = dict(EAG_HEADERS_BASE); h["Access-Token"] = access
    org = _json_ok(_request(s, "POST", ORG_URL, headers=h, json_body={},
                            step="getOrg"), "getOrg")
    aaa_id = _first_id(org.get("result"), ("aaaId", "id"), "aaaId")
    print(f"✅ resolved aaaId={aaa_id}")

    # 3. switch AAA
    data = _json_ok(_request(s, "POST", SWITCH_AAA, headers=h,
                             json_body={"aaaId": str(aaa_id)}, step="switchAaa"),
                    "switchAaa")
    access = _extract_access_token(data, "switchAaa") or access
    h["Access-Token"] = access

    # 4. resolve businessAccountId dynamically
    biz = _json_ok(_request(s, "POST", BIZ_URL, headers=h, json_body={},
                            step="getBiz"), "getBiz")
    biz_id = _first_id(biz.get("result"), ("businessAccountId", "id"), "businessAccountId")
    print(f"✅ resolved businessAccountId={biz_id}")

    # 5. switch business account
    data = _json_ok(_request(s, "POST", SWITCH_BIZ, headers=h,
                             json_body={"businessAccountId": str(biz_id),
                                        "aaaId": str(aaa_id)}, step="switchBiz"),
                    "switchBiz")
    access = _extract_access_token(data, "switchBiz") or access
    h["Access-Token"] = access

    # 6. getAuthCode (establishes the bridge; SESSION cookie lands in jar)
    data = _json_ok(_request(s, "POST", AUTHCODE_URL, headers=h,
                             json_body={"accessToken": access}, step="getAuthCode"),
                    "getAuthCode")
    bridge_access = _extract_access_token(data, "getAuthCode") or access

    # 7. getAhaGameToken -- SESSION cookie carried automatically by the jar.
    #    Also pass access-token as a belt-and-suspenders header.
    dh = {"Accept": "application/json, text/plain, */*",
          "Access-Token": bridge_access,
          "User-Agent": EAG_HEADERS_BASE["User-Agent"]}
    data = _json_ok(_request(s, "GET", AHA_TOKEN_URL, headers=dh, step="getAhaGameToken"),
                    "getAhaGameToken")
    jwt = data.get("result")
    if not isinstance(jwt, str) or not jwt.startswith("eyJ"):
        fail(f"getAhaGameToken did not return a JWT string; got {type(jwt).__name__}: "
             f"{str(jwt)[:80]}")
    jwt = jwt.strip()
    print("✅ got Aha game JWT")
    return s, jwt


def _extract_access_token(data, step):
    r = data.get("result")
    if isinstance(r, dict):
        return r.get("accessToken") or r.get("access_token")
    if isinstance(r, str) and r.startswith("3-"):
        return r
    # some responses put it at top level
    return data.get("accessToken")


def _first_id(result, keys, label):
    """Pull the first record's id from a list/dict result, trying several keys."""
    rows = result
    if isinstance(result, dict):
        rows = result.get("list") or result.get("records") or result.get("data") or []
    if not isinstance(rows, list) or not rows:
        fail(f"could not resolve {label}: result had no list ({str(result)[:120]})")
    rec = rows[0]
    for k in keys:
        if isinstance(rec, dict) and rec.get(k) not in (None, ""):
            return rec[k]
    fail(f"could not find any of {keys} in first record for {label}: {str(rec)[:120]}")


# ------------------------------------------------------------ data pull
def fetch_all(session, jwt, tz, date_start, date_end) -> list[dict]:
    """Generic total-driven pagination for one timezone window."""
    headers = {"token": jwt, "lang": "en", "Accept-Timezone": tz,
               "Accept": "application/json, text/plain, */*",
               "User-Agent": EAG_HEADERS_BASE["User-Agent"]}
    all_rows, page = [], 1
    total = None
    while True:
        params = (f"?gameName=&gamePackage=&dwDateStart={date_start}"
                  f"&dwDateEnd={date_end}&sortColumn=&sortType=&page={page}"
                  f"&limit={PAGE_LIMIT}")
        data = _json_ok(_request(session, "GET", LIST_URL + params, headers=headers,
                                 step=f"list[{tz}] p{page}"), f"list[{tz}] p{page}")
        block = data.get("data") or {}
        rows = block.get("list") or []
        if total is None:
            total = int(block.get("total") or 0)
            pages = max(1, math.ceil(total / PAGE_LIMIT)) if total else 1
            print(f"   [{tz}] total={total} -> {pages} page(s) @ limit {PAGE_LIMIT}")
        all_rows.extend(rows)
        if not rows or len(all_rows) >= total or page >= 10000:
            break
        page += 1
    # bleed-proof: collected count must equal server's declared total
    if total and len(all_rows) != total:
        fail(f"[{tz}] pagination mismatch: collected {len(all_rows)} != total {total}")
    print(f"   [{tz}] collected {len(all_rows)} rows ✅")
    return all_rows


# ------------------------------------------------------------ parsing
def _num(v):
    """null/'' -> None; '50.00%' -> 0.5; '1,234' -> 1234.0; numbers pass through."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    pct = s.endswith("%")
    if pct:
        s = s[:-1]
    try:
        f = float(s)
    except ValueError:
        return None
    return f / 100.0 if pct else f


def _int(v):
    f = _num(v)
    return int(f) if f is not None else None


def normalize(rows, tz, pulled_at) -> list[dict]:
    out = []
    for r in rows:
        pkg = r.get("gamePackage")
        d = r.get("dwDate")
        if not pkg or not d:
            # never silently drop -- a row without a key is a real problem
            fail(f"row missing gamePackage/dwDate: {str(r)[:160]}")
        out.append({
            "dw_date": str(d)[:10],
            "game_package": pkg,
            "game_name": r.get("gameName"),
            "timezone": tz,
            "download_success_pv": _int(r.get("downloadSuccessPv")),
            "install_done_uv": _int(r.get("installDoneUv")),
            "dau": _int(r.get("dau")),
            "init_uv": _int(r.get("initUv")),
            "init_rate": _num(r.get("initRate")),
            "active_ret1": _num(r.get("activeRet1")),
            "init_ret1": _num(r.get("initRet1")),
            "impressions": _int(r.get("impressions")),
            "clicks": _int(r.get("clicks")),
            "earnings_usd": _num(r.get("earnings")),
            "iap_purchase_usd": _num(r.get("iapPurchase")),
            "total_revenue_usd": _num(r.get("totalRevenue")),
            "arpu_ten_thousand": _num(r.get("arpuTenThousand")),
            "avg_duration": _num(r.get("avgDuration")),
            "window_start": None,  # filled by caller
            "window_end": None,
            "pulled_at_utc": pulled_at,
        })
    return out


# ------------------------------------------------------------ BigQuery
def load_bq(rows, run_date):
    local = f"/tmp/dlight_{run_date}.ndjson"
    with open(local, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"💾 wrote {len(rows)} rows -> {local}")
    if DRY_RUN:
        print("🟡 DRY_RUN=1 -> skipping BigQuery")
        return

    from google.cloud import bigquery
    bq = bigquery.Client(project=BQ_PROJECT)
    ds = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    ds.location = BQ_LOCATION
    bq.create_dataset(ds, exists_ok=True)
    print(f"✅ dataset ready: {BQ_PROJECT}.{BQ_DATASET} ({BQ_LOCATION})")

    stg = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_stg"
    tgt = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    schema = [
        bigquery.SchemaField("dw_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("game_package", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_name", "STRING"),
        bigquery.SchemaField("timezone", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("download_success_pv", "INT64"),
        bigquery.SchemaField("install_done_uv", "INT64"),
        bigquery.SchemaField("dau", "INT64"),
        bigquery.SchemaField("init_uv", "INT64"),
        bigquery.SchemaField("init_rate", "FLOAT64"),
        bigquery.SchemaField("active_ret1", "FLOAT64"),
        bigquery.SchemaField("init_ret1", "FLOAT64"),
        bigquery.SchemaField("impressions", "INT64"),
        bigquery.SchemaField("clicks", "INT64"),
        bigquery.SchemaField("earnings_usd", "FLOAT64"),
        bigquery.SchemaField("iap_purchase_usd", "FLOAT64"),
        bigquery.SchemaField("total_revenue_usd", "FLOAT64"),
        bigquery.SchemaField("arpu_ten_thousand", "FLOAT64"),
        bigquery.SchemaField("avg_duration", "FLOAT64"),
        bigquery.SchemaField("window_start", "STRING"),
        bigquery.SchemaField("window_end", "STRING"),
        bigquery.SchemaField("pulled_at_utc", "TIMESTAMP"),
    ]
    cfg = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    with open(local, "rb") as f:
        bq.load_table_from_file(f, stg, job_config=cfg).result()
    print(f"📥 staging loaded: {stg}")

    bq.query(f"""
      CREATE TABLE IF NOT EXISTS `{tgt}` (
        dw_date DATE NOT NULL, game_package STRING NOT NULL, game_name STRING,
        timezone STRING NOT NULL,
        download_success_pv INT64, install_done_uv INT64, dau INT64, init_uv INT64,
        init_rate FLOAT64, active_ret1 FLOAT64, init_ret1 FLOAT64,
        impressions INT64, clicks INT64, earnings_usd FLOAT64,
        iap_purchase_usd FLOAT64, total_revenue_usd FLOAT64,
        arpu_ten_thousand FLOAT64, avg_duration FLOAT64,
        window_start STRING, window_end STRING, pulled_at_utc TIMESTAMP
      ) PARTITION BY dw_date CLUSTER BY game_package, timezone
    """).result()

    merge = bq.query(f"""
      MERGE `{tgt}` T USING `{stg}` S
      ON T.dw_date=S.dw_date AND T.game_package=S.game_package AND T.timezone=S.timezone
      WHEN MATCHED THEN UPDATE SET
        game_name=S.game_name, download_success_pv=S.download_success_pv,
        install_done_uv=S.install_done_uv, dau=S.dau, init_uv=S.init_uv,
        init_rate=S.init_rate, active_ret1=S.active_ret1, init_ret1=S.init_ret1,
        impressions=S.impressions, clicks=S.clicks, earnings_usd=S.earnings_usd,
        iap_purchase_usd=S.iap_purchase_usd, total_revenue_usd=S.total_revenue_usd,
        arpu_ten_thousand=S.arpu_ten_thousand, avg_duration=S.avg_duration,
        window_start=S.window_start, window_end=S.window_end,
        pulled_at_utc=S.pulled_at_utc
      WHEN NOT MATCHED THEN INSERT ROW
    """)
    merge.result()
    print(f"✅ MERGE complete into {tgt} (affected: {merge.num_dml_affected_rows})")


# ------------------------------------------------------------ main
def main():
    pulled_at = dt.datetime.now(dt.timezone.utc).isoformat()
    today = dt.datetime.now(PKT).date()
    if FULL_BACKFILL:
        d_start = BACKFILL_START
    else:
        d_start = (today - dt.timedelta(days=LOOKBACK_DAYS - 1)).isoformat()
    d_end = today.isoformat()
    print(f"🎯 window {d_start} -> {d_end} | timezones={TIMEZONES} "
          f"| {'FULL BACKFILL' if FULL_BACKFILL else 'rolling'}")

    session, jwt = authenticate()

    all_norm = []
    for tz in TIMEZONES:
        rows = fetch_all(session, jwt, tz, d_start, d_end)
        norm = normalize(rows, tz, pulled_at)
        for r in norm:
            r["window_start"], r["window_end"] = d_start, d_end
        all_norm.extend(norm)

    if not all_norm:
        fail("no rows collected across any timezone -- refusing to load")
    load_bq(all_norm, today.isoformat())
    print(f"\n🎯 DONE. {len(all_norm)} rows across {len(TIMEZONES)} timezone(s).")


if __name__ == "__main__":
    main()
