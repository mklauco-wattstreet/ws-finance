"""Ad-hoc CZ A77 outage report -> CSV + self-contained HTML (no deps).

Run:
  docker compose exec -w /app/scripts entsoe-ote-data-uploader python3 outages_report.py [DAYS]

Outputs (visible on host under ./downloads/):
  downloads/outages_cz.csv
  downloads/outages_cz.html
"""
import csv
import io
import sys
import zipfile
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import zoneinfo

from entsoe.client import EntsoeClient

CZ = "10YCZ-CEPS-----N"
NS = {"o": "urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"}
PRAGUE = zoneinfo.ZoneInfo("Europe/Prague")
OUT_DIR = "/app/downloads"
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7

client = EntsoeClient()


def fmt(dt):
    return dt.strftime("%Y%m%d%H%M")


def txt(el, path):
    f = el.find(path, NS)
    return f.text if f is not None else None


def to_prague(iso):
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(PRAGUE)


def fetch_all(start, end):
    """Page through the 200-doc cap via offset; return list of XML bytes."""
    docs = []
    offset = 0
    while True:
        url = (f"{client.base_url}?securityToken={client.security_token}"
               f"&documentType=A77&biddingZone_Domain={CZ}"
               f"&periodStart={fmt(start)}&periodEnd={fmt(end)}&offset={offset}")
        r = client.session.get(url, timeout=120)
        if r.status_code != 200 or r.content[:2] != b"PK":
            break  # non-zip = "no more data" acknowledgement
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        batch = [zf.read(n) for n in zf.namelist()]
        docs.extend(batch)
        if len(batch) < 200:
            break
        offset += 200
    return docs


def parse(xb):
    root = ET.fromstring(xb)
    mrid = txt(root, "o:mRID")
    rev = int(txt(root, "o:revisionNumber") or 0)
    status = txt(root, "o:docStatus/o:value") or "ACTIVE"
    rows = []
    for ts in root.findall("o:TimeSeries", NS):
        nom = txt(ts, "o:production_RegisteredResource.pSRType.powerSystemResources.nominalP")
        nomf = float(nom) if nom else None
        avs = [txt(p, "o:quantity") for ap in ts.findall("o:Available_Period", NS)
               for p in ap.findall("o:Point", NS)]
        av = min((float(a) for a in avs if a), default=None)
        s = txt(ts, "o:start_DateAndOrTime.date")
        e = txt(ts, "o:end_DateAndOrTime.date")
        s_dt = to_prague(s and s + "T" + (txt(ts, "o:start_DateAndOrTime.time") or "00:00:00Z"))
        e_dt = to_prague(e and e + "T" + (txt(ts, "o:end_DateAndOrTime.time") or "00:00:00Z"))
        rows.append({
            "location": txt(ts, "o:production_RegisteredResource.location.name"),
            "fuel": txt(ts, "o:production_RegisteredResource.pSRType.psrType"),
            "business_type": txt(ts, "o:businessType"),
            "status": status,
            "revision": rev,
            "nominal_mw": nomf,
            "available_mw": av,
            "unavailable_mw": (nomf - av) if (nomf is not None and av is not None) else None,
            "start": s_dt,
            "end": e_dt,
            "reason": txt(root, "o:Reason/o:code"),
            "mrid": mrid,
        })
    return rows


end = datetime.now(timezone.utc)
start = end - timedelta(days=DAYS)
docs = fetch_all(start, end)
rows = []
for d in docs:
    try:
        rows.extend(parse(d))
    except Exception as ex:
        print("parse error:", ex)
rows.sort(key=lambda r: (r["start"] or datetime.min.replace(tzinfo=PRAGUE)))
print(f"Fetched {len(docs)} documents -> {len(rows)} event rows over {DAYS} days")

# ---- CSV ----
csv_path = f"{OUT_DIR}/outages_cz.csv"
with open(csv_path, "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["location", "fuel", "business_type", "status", "revision",
                "nominal_mw", "available_mw", "unavailable_mw", "start", "end", "reason", "mrid"])
    for r in rows:
        w.writerow([r["location"], r["fuel"], r["business_type"], r["status"], r["revision"],
                    r["nominal_mw"], r["available_mw"], r["unavailable_mw"],
                    r["start"], r["end"], r["reason"], r["mrid"]])

# ---- Hourly total unavailable MW (ACTIVE only) for the SVG chart ----
hours = []
cur = datetime.now(PRAGUE).replace(minute=0, second=0, microsecond=0) - timedelta(days=DAYS)
last = datetime.now(PRAGUE).replace(minute=0, second=0, microsecond=0)
while cur <= last:
    tot = 0.0
    for r in rows:
        if r["status"] != "ACTIVE":
            continue
        if r["start"] and r["end"] and r["start"] <= cur < r["end"] and r["unavailable_mw"]:
            tot += r["unavailable_mw"]
    hours.append((cur, tot))
    cur += timedelta(hours=1)

peak = max((h[1] for h in hours), default=1) or 1
CH, CW, BARW = 320, max(900, len(hours) * 6), max(3, int(900 / max(len(hours), 1)))
bars = []
for i, (h, v) in enumerate(hours):
    bh = (v / peak) * (CH - 40)
    x = 50 + i * BARW
    color = "#c0392b" if v > peak * 0.66 else ("#e67e22" if v > peak * 0.33 else "#2980b9")
    bars.append(f'<rect x="{x}" y="{CH-20-bh:.0f}" width="{BARW-1}" height="{bh:.0f}" '
                f'fill="{color}"><title>{h:%Y-%m-%d %H:%M}  {v:.0f} MW</title></rect>')
# y gridlines
grid = []
for frac in (0, 0.25, 0.5, 0.75, 1.0):
    y = CH - 20 - frac * (CH - 40)
    grid.append(f'<line x1="50" y1="{y:.0f}" x2="{CW}" y2="{y:.0f}" stroke="#eee"/>'
                f'<text x="5" y="{y+4:.0f}" font-size="10" fill="#888">{frac*peak:.0f}</text>')
svg = (f'<svg width="{CW}" height="{CH}" xmlns="http://www.w3.org/2000/svg">'
       + "".join(grid) + "".join(bars) + "</svg>")

# ---- table rows ----
def cell(v):
    return "" if v is None else (f"{v}")

trs = []
for r in rows:
    badge = '#27ae60' if r["status"] == "ACTIVE" else '#999'
    trs.append(
        f"<tr><td>{cell(r['location'])}</td><td>{cell(r['fuel'])}</td>"
        f"<td>{cell(r['business_type'])}</td>"
        f"<td style='color:{badge}'>{cell(r['status'])}</td><td>{r['revision']}</td>"
        f"<td class=n>{cell(r['nominal_mw'])}</td><td class=n>{cell(r['available_mw'])}</td>"
        f"<td class=n><b>{cell(r['unavailable_mw'])}</b></td>"
        f"<td>{cell(r['start'])}</td><td>{cell(r['end'])}</td><td>{cell(r['reason'])}</td></tr>")

now_active = sum(r["unavailable_mw"] or 0 for r in rows
                 if r["status"] == "ACTIVE" and r["start"] and r["end"]
                 and r["start"] <= datetime.now(PRAGUE) < r["end"])

html = f"""<!doctype html><meta charset=utf-8>
<title>CZ A77 Outages</title>
<style>
body{{font:13px system-ui,Arial;margin:24px;color:#222}}
h1{{font-size:18px}} .sub{{color:#666;margin-bottom:16px}}
table{{border-collapse:collapse;width:100%;margin-top:16px}}
th,td{{border-bottom:1px solid #eee;padding:4px 8px;text-align:left}}
th{{background:#fafafa;position:sticky;top:0;cursor:pointer}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}}
.card{{background:#f7f9fb;border:1px solid #e3e8ee;border-radius:8px;padding:12px 16px;display:inline-block;margin-right:12px}}
.big{{font-size:22px;font-weight:700}}
</style>
<h1>CZ Generation/Production Outages (ENTSO-E A77)</h1>
<div class=sub>Window: last {DAYS} days &middot; {len(docs)} documents &middot; {len(rows)} event rows &middot; generated {datetime.now(PRAGUE):%Y-%m-%d %H:%M %Z}</div>
<div class=card><div>Unavailable NOW (active)</div><div class=big>{now_active:.0f} MW</div></div>
<div class=card><div>Peak hourly (window)</div><div class=big>{peak:.0f} MW</div></div>
<h3>Total ACTIVE unavailable MW per hour</h3>
{svg}
<h3>Event rows (sorted by start)</h3>
<table id=t><thead><tr>
<th>Location</th><th>Fuel</th><th>BT</th><th>Status</th><th>Rev</th>
<th>Nominal</th><th>Available</th><th>Unavailable</th><th>Start</th><th>End</th><th>Reason</th>
</tr></thead><tbody>
{''.join(trs)}
</tbody></table>
<script>
document.querySelectorAll('th').forEach((th,i)=>th.onclick=()=>{{
 const tb=document.querySelector('tbody'),rs=[...tb.rows];
 const num=i>=4&&i<=7;th._d=!th._d;
 rs.sort((a,b)=>{{let x=a.cells[i].innerText,y=b.cells[i].innerText;
  if(num){{x=parseFloat(x)||0;y=parseFloat(y)||0;return th._d?x-y:y-x;}}
  return th._d?x.localeCompare(y):y.localeCompare(x);}});
 rs.forEach(r=>tb.appendChild(r));
}});
</script>
"""
html_path = f"{OUT_DIR}/outages_cz.html"
with open(html_path, "w") as fh:
    fh.write(html)

print(f"WROTE: {csv_path}")
print(f"WROTE: {html_path}")
print(f"Unavailable NOW (active): {now_active:.0f} MW | peak hourly: {peak:.0f} MW")
