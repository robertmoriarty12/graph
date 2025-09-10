# === CONFIG ===
TENANT_ID     = ""
CLIENT_ID     = ""
CLIENT_SECRET = ""

DAY_UTC       = "2025-09-01"         # UTC day to export (YYYY-MM-DD)
SLICE_MINUTES = 1                    # time slice length (1–5 recommended for big tenants)
PAGE_SIZE     = 100000               # AH max rows per page
OUT_DIR       = "out_dne_2025-09-01" # output folder for NDJSON files

# === SCRIPT ===
import os, time, json, datetime, typing, textwrap
import requests

TOKEN_URL  = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
RUN_URL     = "https://graph.microsoft.com/v1.0/security/runHuntingQuery"

os.makedirs(OUT_DIR, exist_ok=True)

def get_token() -> str:
    print("[*] Requesting access token...")
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": GRAPH_SCOPE,
        "grant_type": "client_credentials",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    print(f"[+] Token response status: {r.status_code} {r.reason}")
    ct = r.headers.get("Content-Type", "")
    if ct:
        print(f"[+] Token response Content-Type: {ct}")
    if not r.ok:
        print("[-] Token error body (first 500 chars):\n" + r.text[:500])
        r.raise_for_status()
    try:
        j = r.json()
    except Exception:
        print("[-] Token response was not JSON. Body (first 500):\n" + r.text[:500])
        raise
    token = j.get("access_token")
    if not token:
        print("[-] No access_token in response. Full JSON (truncated):\n" + json.dumps(j)[:500])
        raise SystemExit(1)
    print("[+] Got access token (truncated):", token[:20], "...")
    return token

def run_query(token: str, kql: str) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"Query": kql}
    print("[*] Running query with KQL:\n" + textwrap.indent(kql, "    "))
    backoff = 2
    while True:
        resp = requests.post(RUN_URL, headers=headers, json=payload, timeout=175)  # < 3 min API limit
        print(f"[+] Query response status: {resp.status_code} {resp.reason}")
        if resp.status_code in (429, 500, 502, 503, 504):
            ra = resp.headers.get("Retry-After")
            wait = backoff if not ra else max(1, int(ra)) if ra.isdigit() else backoff
            print(f"[!] Throttled/server error. Waiting {wait}s then retrying...")
            time.sleep(min(wait, 30))
            backoff = min(backoff * 2, 60)
            continue
        if not resp.ok:
            print("[-] Query error body (first 500 chars):\n" + resp.text[:500])
            resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            print("[-] Query response was not JSON. Body (first 500):\n" + resp.text[:500])
            raise

def build_kql(slice_start: datetime.datetime,
              slice_end: datetime.datetime,
              last_ts: typing.Optional[str],
              last_report_id: typing.Optional[str]) -> str:
    s = slice_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    e = slice_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = []
    lines.append(f"let SliceStart=datetime({s});")
    lines.append(f"let SliceEnd=datetime({e});")
    lines.append(f"let PageSize={PAGE_SIZE};")
    lines.append("DeviceNetworkEvents")
    lines.append("| where Timestamp between (SliceStart..SliceEnd)")
    if last_ts is not None and last_report_id is not None:
        lines.append(f"| where Timestamp>datetime({last_ts}) or (Timestamp==datetime({last_ts}) and ReportId>\"{last_report_id}\")")
    lines.append("| order by Timestamp asc,ReportId asc")
    lines.append("| take PageSize")
    return "\n".join(lines)

def rows_from_graph(result: dict):
    """
    Graph returns:
    {
      "@odata.context": "...huntingQueryResults",
      "schema": [...],
      "results": [ { "ColA": "...", ... }, ... ]
    }
    """
    return result.get("results", []) or []

def drain_slice(token: str, slice_start: datetime.datetime, slice_end: datetime.datetime) -> int:
    page = 1
    total = 0
    last_ts = None
    last_rid = None
    while True:
        print(f"[*] Slice {slice_start.isoformat()} → {slice_end.isoformat()}, page {page}")
        kql = build_kql(slice_start, slice_end, last_ts, last_rid)
        data = run_query(token, kql)
        results = rows_from_graph(data)
        print(f"[+] Retrieved {len(results)} rows")
        if not results:
            print("[*] No rows; slice complete.")
            break
        tag = f"DNE_{slice_start.strftime('%Y-%m-%dT%H-%M')}_p{page}"
        path = os.path.join(OUT_DIR, f"{tag}.ndjson")
        with open(path, "w", encoding="utf-8") as f:
            for row in results:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[+] Wrote file {path}")
        total += len(results)
        last = results[-1]
        last_ts = last.get("Timestamp")
        last_rid = str(last.get("ReportId"))
        if not last_ts or not last_rid:
            print("[!] Missing anchors (Timestamp/ReportId). Stopping this slice.")
            break
        page += 1
        if len(results) < PAGE_SIZE:
            print("[*] Final page for this slice (under PageSize).")
            break
    return total

def iter_day(day_utc: str, minutes: int):
    start = datetime.datetime.fromisoformat(day_utc + "T00:00:00+00:00").astimezone(datetime.timezone.utc)
    end = start + datetime.timedelta(days=1)
    cur = start
    step = datetime.timedelta(minutes=minutes)
    while cur < end:
        nxt = min(cur + step, end)
        yield cur, nxt
        cur = nxt

def main():
    try:
        token = get_token()
    except Exception as e:
        print("[-] Failed to obtain token:", e)
        raise
    total = 0
    for s, e in iter_day(DAY_UTC, SLICE_MINUTES):
        print(f"\n=== Processing slice {s.isoformat()} → {e.isoformat()} ===")
        try:
            total += drain_slice(token, s, e)
        except Exception as ex:
            print("[-] Error during slice:", ex)
            # You can choose to continue or re-raise. Here we continue.
            continue
        time.sleep(0.2)  # gentle pacing to avoid bursting limits
    print(f"\n[✓] Done. Rows exported: {total}. Files in: {OUT_DIR}")

if __name__ == "__main__":
    main()
