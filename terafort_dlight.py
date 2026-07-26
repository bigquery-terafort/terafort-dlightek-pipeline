#!/usr/bin/env python3
"""
================================================================================
 D-LIGHT (Dlightek / Transsion) APK GAME DATA  ->  BIGQUERY   ·   v2
================================================================================
 REPO: bigquery-terafort/terafort-dlightek-pipeline

 DATA HAAL: ✅ sehatmand. Ye script sab se behtar likhi hui thi —
   `if not all_norm: fail(...)` guard aur staging+atomic MERGE pehle se the.
   (Yehi do cheezein baaki 11 scripts mein nahi thin.)

 🟡 v2 KYUN — PAGINATION TIME-BOMB:
       aaj ka pull: 858 rows ÷ 2 timezones = 429 rows per tz
       PAGE_LIMIT (purana default)         = 500
                                              ────
                                              sirf 71 rows ki gunjaish

    Aur bug:
        if total is None:
            total = int(block.get("total") or 0)     # API ne total na diya → 0
        ...
        if not rows or len(all_rows) >= total or ...:
            break                                    # 429 >= 0 → TRUE → page 1 pe ruk gaya
        ...
        if total and len(all_rows) != total:         # total=0 falsy → check SKIP
            fail(...)

    Yani 'total' field gayab ho (ya naam badle) to CHUP-CHAAP sirf pehla page
    aata aur assertion bhi khamosh rehti.

 v2 KE FIX:
   🛡️ 1. 'total' gayab + rows maujood → foran fail (truncation na chhupe)
   🛡️ 2. page poora bhara ho to aage barho (total==0 pe bhi sahi bartao)
   🛡️ 3. assertion ab HAR HAAL mein chalti hai
   🛡️ 4. PAGE_LIMIT default 500 → 1000 (headroom)

 AUTH CHAIN (sab v1 jaisa):
   login → getOrg → switchAaa → getBiz → switchBiz → getAuthCode →
   callback (SESSION cookie) → getAhaGameToken (24h JWT) → adInstallPay/list

 LAND -> BigQuery MERGE on (dw_date, game_package, timezone)
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
# 🛡️ v2: 500 → 1000. Aaj 429 rows/tz hain — 500 pe sirf 71 rows door.
PAGE_LIMIT = int(env("PAGE_LIMIT", "1000"))
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
    fail(f"[{step}] exhausted retries")


def _json_ok(resp, step):
    try:
        data = resp.json()
    except ValueError:
        fail(f"[{step}] non-JSON response: {resp.text[:300]}")
    code = str(data.get("code", ""))
    result = data.get("result")
    if isinstance(result, list) and any("not logged in" in str(x).lower()
                                        or "未登录" in str(x) for x in result):
        fail(f"[{step}] session not authenticated (result={result}). "
             f"This should not happen on a fresh login -- check credentials.")
    if code not in ("0", "200"):
        fail(f"[{step}] API code={code} message={data.get('message')!r}")
    return data


# ------------------------------------------------------------ auth chain
def authenticate() -> tuple:
    """Run the full SSO chain; return (session, aha_jwt)."""
    s = requests.Session()

    body = {"email": EMAIL, "password": md5_upper(PASSWORD),
            "captchaKey": "", "emailCaptcha": ""}
    data = _json_ok(_request(s, "POST", LOGIN_URL, headers=EAG_HEADERS_BASE,
                             json_body=body, step="login"), "login")
    access = _require_access_token(data, "login")
    print("✅ login ok")

    h = dict(EAG_HEADERS_BASE); h["Access-Token"] = access
    org = _json_ok(_request(s, "POST", ORG_URL, headers=h,
                            json_body={"paging": {"currentPage": 1, "pageSize": 999}},
                            step="getOrg"), "getOrg")
    aaa_id = _first_id(_payload(org), ("aaaId", "id"), "aaaId")
    print(f"✅ resolved aaaId={aaa_id}")

    data = _json_ok(_request(s, "POST", SWITCH_AAA, headers=h,
                             json_body={"aaaId": str(aaa_id)}, step="switchAaa"),
                    "switchAaa")
    access = _require_access_token(data, "switchAaa", prior=access)
    h["Access-Token"] = access

    biz = _json_ok(_request(s, "POST", BIZ_URL, headers=h,
                            json_body={"businessTypes": ["19"],
                                       "businessAreaTypes": [1],
                                       "paging": {"currentPage": 1, "pageSize": 999}},
                            step="getBiz"), "getBiz")
    biz_id = _first_id(_payload(biz), ("businessAccountId", "id"), "businessAccountId")
    biz_aaa_id = _record_field(_payload(biz), ("aaaId",), default=aaa_id)
    print(f"✅ resolved businessAccountId={biz_id} (aaaId={biz_aaa_id})")

    data = _json_ok(_request(s, "POST", SWITCH_BIZ, headers=h,
                             json_body={"businessAccountId": str(biz_id),
                                        "aaaId": str(biz_aaa_id)}, step="switchBiz"),
                    "switchBiz")
    access = _require_access_token(data, "switchBiz", prior=access)
    h["Access-Token"] = access

    data = _json_ok(_request(s, "POST", AUTHCODE_URL, headers=h,
                             json_body={"accessToken": access}, step="getAuthCode"),
                    "getAuthCode")
    auth_code = _extract_auth_code(data)
    bridge_access = _require_access_token(data, "getAuthCode", prior=access)

    _establish_dlight_session(s, auth_code, bridge_access)

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
    candidates = []
    r = data.get("result")
    if isinstance(r, dict):
        candidates += [r.get("accessToken"), r.get("access_token"),
                       r.get("token"), r.get("accessTokenStr")]
    elif isinstance(r, str) and r.startswith("3-"):
        candidates.append(r)
    d = data.get("data")
    if isinstance(d, dict):
        candidates += [d.get("accessToken"), d.get("access_token"), d.get("token")]
    candidates += [data.get("accessToken"), data.get("access_token"), data.get("token")]
    for c in candidates:
        if isinstance(c, str) and c.startswith("3-"):
            return c
    return None


def _require_access_token(data, step, prior=None):
    tok = _extract_access_token(data, step) or prior
    if not tok:
        fail(f"[{step}] no access-token in response and no prior token to reuse. "
             f"Response keys={list(data)}; result type="
             f"{type(data.get('result')).__name__}. Raw(first 300): "
             f"{json.dumps(data)[:300]}")
    return tok


CALLBACK_URL = "https://dev.dlightek.com/api/user/callback"
CALLBACK_PATH = "https://dev.dlightek.com/admin-data-report/apkGameData"


def _extract_auth_code(data):
    for container in (data.get("data"), data.get("result"), data):
        if isinstance(container, dict):
            for k in ("authCode", "code", "auth_code", "ticket"):
                v = container.get(k)
                if isinstance(v, str) and v and not v.startswith("3-"):
                    return v
        elif isinstance(container, str) and container and not container.startswith("3-"):
            return container
    return None


def _establish_dlight_session(s, auth_code, access_fallback):
    ua = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "User-Agent": EAG_HEADERS_BASE["User-Agent"],
          "Referer": "https://portal.lionnan.com/"}
    from urllib.parse import quote
    codes = [c for c in (auth_code, access_fallback) if c]
    override = os.environ.get("DLIGHT_BRIDGE_URL", "").strip()

    tried = []
    for code in codes:
        if override:
            url = override.replace("{CODE}", code).replace("{T}", code)
        else:
            url = (f"{CALLBACK_URL}?path={quote(CALLBACK_PATH, safe='')}"
                   f"&authCode={code}&langType=en&businessType=19")
        try:
            s.get(url, headers=ua, timeout=HTTP_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            tried.append(f"code={code[:12]}.. -> network error {exc}")
            continue
        if _verify_dlight_session(s):
            print(f"✅ dlightek SESSION established via callback "
                  f"(authCode {code[:10]}..)")
            return
        tried.append(f"code={code[:12]}.. -> profile says not logged in")
    fail("could not establish dlightek SESSION via callback. Tried: "
         + "; ".join(tried) + ". If the authCode field name changed, set "
         "DLIGHT_BRIDGE_URL with '{CODE}' as the placeholder.")


def _verify_dlight_session(s):
    ua = {"Accept": "application/json, text/plain, */*",
          "User-Agent": EAG_HEADERS_BASE["User-Agent"],
          "Referer": "https://dev.dlightek.com/"}
    try:
        p = s.get("https://dev.dlightek.com/api/user/profile", headers=ua,
                  timeout=HTTP_TIMEOUT)
        pj = p.json() if p.status_code == 200 else {}
    except (requests.RequestException, ValueError):
        return False
    res = pj.get("result")
    return str(pj.get("code")) == "200" and isinstance(res, dict) and bool(res.get("email"))


def _payload(data):
    r = data.get("result")
    if r is not None:
        return r
    return data.get("data")


def _first_id(result, keys, label):
    rec = _first_record(result, label)
    for k in keys:
        if isinstance(rec, dict) and rec.get(k) not in (None, ""):
            return rec[k]
    fail(f"could not find any of {keys} in first record for {label}: {str(rec)[:160]}")


def _record_field(result, keys, default=None):
    try:
        rec = _first_record(result, "field-lookup")
    except SystemExit:
        return default
    for k in keys:
        if isinstance(rec, dict) and rec.get(k) not in (None, ""):
            return rec[k]
    return default


def _first_record(result, label):
    rows = result
    if isinstance(result, dict):
        rows = (result.get("list") or result.get("records") or result.get("rows")
                or result.get("data") or [])
        if isinstance(rows, dict):
            rows = (rows.get("list") or rows.get("records") or rows.get("rows") or [])
    if not isinstance(rows, list) or not rows:
        fail(f"could not resolve {label}: result had no list ({str(result)[:160]})")
    return rows[0]


# ------------------------------------------------------------ data pull
def fetch_all(session, jwt, tz, date_start, date_end) -> list:
    """v2: total gayab ho to CHILLAO; page bhar jaye to aage barho;
    assertion har haal mein chale.
    """
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
            raw_total = block.get("total")
            # 🛡️ FIX: 'total' ka gayab hona CHUP-CHAAP truncation ban jata tha.
            #    total=0 → `len(all_rows) >= 0` hamesha True → page 1 pe break,
            #    aur neeche wala assertion bhi `if total` ki wajah se skip.
            if raw_total is None and rows:
                fail(f"[{tz}] API response has no 'total' field but returned "
                     f"{len(rows)} rows — pagination cannot be verified. "
                     f"Refusing to load a possibly-truncated result. "
                     f"(API shape changed? keys={list(block)})")
            total = int(raw_total or 0)
            pages = max(1, math.ceil(total / PAGE_LIMIT)) if total else 1
            print(f"   [{tz}] total={total} -> {pages} page(s) @ limit {PAGE_LIMIT}")

        all_rows.extend(rows)

        # 🛡️ FIX: total==0 pe bhi sahi bartao — page poora bhara ho to aage barho
        if (not rows
                or (total and len(all_rows) >= total)
                or len(rows) < PAGE_LIMIT
                or page >= 10000):
            break
        page += 1

    # 🛡️ FIX: assertion ab HAR HAAL mein (v1 ka `if total and ...` skip ho jata tha)
    if len(all_rows) != total:
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


def normalize(rows, tz, pulled_at) -> list:
    out = []
    for r in rows:
        pkg = r.get("gamePackage")
        d = r.get("dwDate")
        if not pkg or not d:
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
            "window_start": None,
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
          f"| {'FULL BACKFILL' if FULL_BACKFILL else 'rolling'} "
          f"| PAGE_LIMIT={PAGE_LIMIT}")

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
