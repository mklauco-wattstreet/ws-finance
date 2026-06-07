"""Cumulative CZ outage MW per 1-hour delivery period (anchored to outage window).

Run:
  docker compose exec -w /app/scripts entsoe-ote-data-uploader python3 outages_hourly.py [DAYS_BACK] [DAYS_FWD]

Outputs (host ./downloads/):
  downloads/outages_cz_hourly.csv   delivery_hour, total_out_mw, n_plants, breakdown
  downloads/outages_cz_hourly.html  bar chart + table
"""
import csv, io, sys, zipfile
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import zoneinfo
from collections import defaultdict
from entsoe.client import EntsoeClient

CZ="10YCZ-CEPS-----N"; NS={"o":"urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"}
PR=zoneinfo.ZoneInfo("Europe/Prague"); OUT="/app/downloads"; c=EntsoeClient()
FUEL={"B02":"Lignite","B05":"Hard coal","B12":"Hydro res","B04":"Gas","B14":"Nuclear","B10":"Hydro pump"}
DAYS_BACK=int(sys.argv[1]) if len(sys.argv)>1 else 3
DAYS_FWD =int(sys.argv[2]) if len(sys.argv)>2 else 3

def f(d): return d.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
def t(e,p):
    x=e.find(p,NS); return x.text if x is not None else None
def loc(iso): return datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(PR) if iso else None

def fetch(start,end):
    docs=[]; off=0
    while True:
        url=(f"{c.base_url}?securityToken={c.security_token}&documentType=A77"
             f"&biddingZone_Domain={CZ}&periodStart={f(start)}&periodEnd={f(end)}&offset={off}")
        r=c.session.get(url,timeout=120)
        if r.status_code!=200 or r.content[:2]!=b"PK": break
        zf=zipfile.ZipFile(io.BytesIO(r.content)); b=[zf.read(n) for n in zf.namelist()]
        docs+=b
        if len(b)<200: break
        off+=200
    return docs

now=datetime.now(PR)
win_s=(now-timedelta(days=DAYS_BACK)).replace(minute=0,second=0,microsecond=0)
win_e=(now+timedelta(days=DAYS_FWD)).replace(minute=0,second=0,microsecond=0)
# fetch wide enough to catch long planned outages overlapping the window
docs=fetch(win_s-timedelta(days=30), win_e)

rows=[]
for xb in docs:
    root=ET.fromstring(xb); status=t(root,"o:docStatus/o:value") or "ACTIVE"; rev=int(t(root,"o:revisionNumber") or 0)
    for ts in root.findall("o:TimeSeries",NS):
        nom=t(ts,"o:production_RegisteredResource.pSRType.powerSystemResources.nominalP"); nomf=float(nom) if nom else None
        avs=[t(p,"o:quantity") for ap in ts.findall("o:Available_Period",NS) for p in ap.findall("o:Point",NS)]
        av=min((float(a) for a in avs if a),default=None)
        s=loc(t(ts,"o:start_DateAndOrTime.date") and t(ts,"o:start_DateAndOrTime.date")+"T"+(t(ts,"o:start_DateAndOrTime.time") or "00:00Z"))
        e=loc(t(ts,"o:end_DateAndOrTime.date") and t(ts,"o:end_DateAndOrTime.date")+"T"+(t(ts,"o:end_DateAndOrTime.time") or "00:00Z"))
        if not (s and e and nomf is not None and av is not None): continue
        rows.append(dict(locn=t(ts,"o:production_RegisteredResource.location.name"),fuel=t(ts,"o:production_RegisteredResource.pSRType.psrType"),
            bt=t(ts,"o:businessType"),status=status,rev=rev,nom=nomf,av=av,un=nomf-av,s=s,e=e))

# ---- per delivery hour: dedup per plant (active, latest rev / max unavailable), sum ----
series=[]   # (hour, total_mw, n_plants, fuel_breakdown, plant_list)
h=win_s
while h<win_e:
    he=h+timedelta(hours=1)
    perplant={}
    for x in rows:
        if x["status"]!="ACTIVE": continue
        if x["s"]<he and x["e"]>h:   # outage covers this delivery hour
            k=x["locn"]
            if k not in perplant or (x["rev"],x["un"])>(perplant[k]["rev"],perplant[k]["un"]):
                perplant[k]=x
    tot=sum(v["un"] for v in perplant.values())
    fb=defaultdict(float)
    for v in perplant.values(): fb[FUEL.get(v["fuel"],v["fuel"])]+=v["un"]
    series.append((h,tot,len(perplant),dict(fb),
                   ", ".join(f"{(v['locn'] or '?')[:10]}:{v['un']:.0f}" for v in sorted(perplant.values(),key=lambda z:-z['un']))))
    h=he

# ---- CSV ----
csv_path=f"{OUT}/outages_cz_hourly.csv"
with open(csv_path,"w",newline="") as fh:
    w=csv.writer(fh); w.writerow(["delivery_hour_start","delivery_hour_end","total_out_mw","n_plants","by_fuel","plants"])
    for hr,tot,n,fb,pl in series:
        w.writerow([f"{hr:%Y-%m-%d %H:%M}",f"{hr+timedelta(hours=1):%H:%M}",round(tot),n,
                    "; ".join(f"{k}:{round(v)}" for k,v in fb.items()),pl])

# ---- HTML bar chart ----
peak=max((s[1] for s in series),default=1) or 1
BARW=max(6,int(1100/max(len(series),1))); CW=max(1100,len(series)*BARW); CH=340
bars=[]; ticks=[]
for i,(hr,tot,n,fb,pl) in enumerate(series):
    bh=(tot/peak)*(CH-50); x=55+i*BARW
    col="#c0392b" if hr<=now<hr+timedelta(hours=1) else ("#888" if hr<now else "#2980b9")
    bars.append(f'<rect x="{x}" y="{CH-25-bh:.0f}" width="{BARW-1}" height="{bh:.0f}" fill="{col}">'
                f'<title>{hr:%Y-%m-%d %H:%M}  {tot:.0f} MW ({n} plants)\n{pl}</title></rect>')
    if hr.hour==0: ticks.append(f'<text x="{x}" y="{CH-8}" font-size="9" fill="#666">{hr:%m-%d}</text>')
grid=[]
for fr in (0,.25,.5,.75,1):
    y=CH-25-fr*(CH-50); grid.append(f'<line x1="55" y1="{y:.0f}" x2="{CW}" y2="{y:.0f}" stroke="#eee"/>'
        f'<text x="5" y="{y+4:.0f}" font-size="10" fill="#888">{fr*peak:.0f}</text>')
svg=f'<svg width="{CW}" height="{CH}" xmlns="http://www.w3.org/2000/svg">{"".join(grid)}{"".join(bars)}{"".join(ticks)}</svg>'

trs=[]
for hr,tot,n,fb,pl in series:
    hot = hr<=now<hr+timedelta(hours=1)
    trs.append(f"<tr{' style=background:#fdecea' if hot else ''}><td>{hr:%Y-%m-%d %H:%M}-{hr+timedelta(hours=1):%H:%M}</td>"
               f"<td class=n><b>{tot:.0f}</b></td><td class=n>{n}</td>"
               f"<td>{'; '.join(f'{k}:{round(v)}' for k,v in fb.items())}</td><td style='color:#666'>{pl}</td></tr>")
html=f"""<!doctype html><meta charset=utf-8><title>CZ hourly outage MW</title>
<style>body{{font:13px system-ui,Arial;margin:24px}}h1{{font-size:18px}}.sub{{color:#666}}
table{{border-collapse:collapse;width:100%;margin-top:14px}}th,td{{border-bottom:1px solid #eee;padding:4px 8px;text-align:left}}
th{{background:#fafafa;position:sticky;top:0}}td.n{{text-align:right;font-variant-numeric:tabular-nums}}</style>
<h1>CZ total outage capacity per 1-hour delivery period (A77, anchored to outage window)</h1>
<div class=sub>{win_s:%Y-%m-%d %H:%M} &rarr; {win_e:%Y-%m-%d %H:%M} &middot; red=current hour, grey=past, blue=future &middot; generated {now:%Y-%m-%d %H:%M %Z}</div>
{svg}
<table><thead><tr><th>Delivery hour (Prague)</th><th>Total OUT MW</th><th>#plants</th><th>By fuel</th><th>Plants (MW)</th></tr></thead>
<tbody>{''.join(trs)}</tbody></table>"""
with open(f"{OUT}/outages_cz_hourly.html","w") as fh: fh.write(html)

print(f"hours: {len(series)} | peak: {peak:.0f} MW")
print(f"WROTE {csv_path}")
print(f"WROTE {OUT}/outages_cz_hourly.html")
print("\nsample (delivery hour -> total MW out):")
for hr,tot,n,fb,pl in series:
    if hr>=now-timedelta(hours=3) and hr<=now+timedelta(hours=6):
        mark="  <== NOW" if hr<=now<hr+timedelta(hours=1) else ""
        print(f"  {hr:%m-%d %H:%M}-{hr+timedelta(hours=1):%H:%M}  {tot:5.0f} MW  ({n} plants){mark}")
