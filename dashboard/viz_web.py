import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BASE_NAMES = {
    "BA": "BART", "SIM": "Simulator", "WAYMO": "Waymo (autonomous)",
    "ZOOX": "Zoox (autonomous)", "RG": "Regional feed (agencies not split)",
}
AUTONOMOUS = {"WAYMO", "ZOOX"}
INCLUDE_SIM = os.getenv("INCLUDE_SIM") == "1"   # simulated data is hidden unless you ask for it
INCLUDE_REPLAY = os.getenv("INCLUDE_REPLAY") == "1"   # replayed recordings are hidden unless you ask for them
NAMES_FILE = "data/agency_names.json"    # written by ingestion/producer_all.py
SHAPES_FILE = "data/route_shapes.json"   # written by ingestion/fetch_routes.py
BUCKET = 10      # seconds per timeline bucket
BUCKETS = 60     # 60 x 10 s = last 10 minutes

lock = threading.Lock()
trails = defaultdict(lambda: deque(maxlen=30))
last_route = {}
last_seen = {}   # (source, vehicle) -> time of its newest report
recent = deque(maxlen=300)   # newest raw messages, for the on-page console
route_counts = defaultdict(int)
timeline = defaultdict(lambda: defaultdict(int))
sources = defaultdict(lambda: {"events": 0, "vehicles": set(), "last": 0, "ages": deque(maxlen=200)})
delays = defaultdict(lambda: {"n": 0, "sum": 0, "max": None, "trips": set()})
_names = {"t": 0, "v": dict(BASE_NAMES)}


def names():
    if time.time() - _names["t"] > 30:
        merged = {}
        try:
            with open(NAMES_FILE) as f:
                merged.update(json.load(f))
        except Exception:
            pass
        merged.update(BASE_NAMES)
        _names["v"], _names["t"] = merged, time.time()
    return _names["v"]


def kind(src):
    if src.endswith("(replay)"):
        return "Replay (not live)"
    if src in AUTONOMOUS:
        return "Autonomous"
    if src == "SIM":
        return "Simulated"
    return "Transit"


def consume():
    c = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                  "group.id": f"viz-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest"})
    c.subscribe(["vehicle-positions", "trip-updates"] + (["replay-positions", "replay-trips"] if INCLUDE_REPLAY else []))
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            e = json.loads(msg.value())
        except Exception:
            continue
        src = e.get("source") or "unknown"
        if src == "SIM" and not INCLUDE_SIM:
            continue
        if dict(msg.headers() or []).get("origin") == b"replay" or msg.topic().startswith("replay-") or src in ("WAYMO", "ZOOX", "ARGO", "AV"):   # no live feed exists for these
            if not INCLUDE_REPLAY:
                continue            # a recording is not live data
            src = f"{src} (replay)"
        try:
            raw_key = msg.key()
            key_txt = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key or "")
        except Exception:
            key_txt = ""
        with lock:
            recent.append({"t": time.time(), "topic": msg.topic(), "key": key_txt, "value": e})
        if msg.topic() in ("trip-updates", "replay-trips"):
            d = e.get("delay_sec")
            if d is None:
                continue
            with lock:
                s = delays[src]
                s["n"] += 1
                s["sum"] += d
                s["max"] = d if s["max"] is None else max(s["max"], d)
                s["trips"].add(e.get("trip_id"))
            continue
        lat, lon = e.get("lat"), e.get("lon")
        if lat is None or lon is None or (lat == 0 and lon == 0):
            continue
        vid = e.get("vehicle_id") or "?"
        route = e.get("route_id") or "unknown"
        bucket = int(time.time() // BUCKET) * BUCKET
        with lock:
            trails[(src, vid)].append([lon, lat])
            last_route[(src, vid)] = route
            last_seen[(src, vid)] = max(last_seen.get((src, vid), 0), (e.get("event_ts") or int(time.time() * 1000)) / 1000)
            route_counts[(src, route)] += 1
            timeline[src][bucket] += 1
            s = sources[src]
            s["events"] += 1
            s["vehicles"].add(vid)
            s["last"] = max(s["last"], (e.get("event_ts") or 0) / 1000)       # event_ts is milliseconds (B4)
            if e.get("ingest_ts") and e.get("event_ts"):
                s["ages"].append((e["ingest_ts"] - e["event_ts"]) / 1000)


def snapshot():
    now_bucket = int(time.time() // BUCKET) * BUCKET
    nm = names()
    with lock:
        top_routes = sorted(route_counts.items(), key=lambda kv: -kv[1])[:15]
        return {
            "names": {k: nm.get(k, k) for k in set(sources) | set(delays)},
            "kinds": {k: kind(k) for k in set(sources) | set(delays)},
            "sources": {k: {"events": v["events"], "vehicles": len(v["vehicles"]), "last": v["last"],
                            "age": (sum(v["ages"]) / len(v["ages"])) if v["ages"] else None}
                        for k, v in sources.items()},
            "delays": {k: {"n": v["n"], "trips": len(v["trips"]), "avg": v["sum"] / v["n"], "max": v["max"]}
                       for k, v in delays.items() if v["n"]},
            "trails": [{"src": k[0], "id": k[1], "route": last_route.get(k), "pts": list(p)[-12:]} for k, p in trails.items() if last_seen.get(k, 0) > time.time() - 900],
            "messages": list(recent)[-80:],
            "routes": [{"src": k[0], "route": k[1], "n": n} for k, n in top_routes],
            "timeline": {src: [tl.get(now_bucket - BUCKET * i, 0) for i in range(BUCKETS - 1, -1, -1)]
                         for src, tl in timeline.items()},
        }


def shapes_json():
    try:
        with open(SHAPES_FILE, "rb") as f:
            return f.read()
    except Exception:
        return b"{}"


PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Fleet telemetry live</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
body{font-family:-apple-system,Helvetica,Arial,sans-serif;margin:20px;background:#fafafa;color:#222}
h1{font-size:20px;margin:0 0 4px} #stats{color:#555;margin-bottom:14px}
.row{display:flex;gap:20px;flex-wrap:wrap;align-items:flex-start;margin-bottom:20px}
.card{background:#fff;border:1px solid #ddd;border-radius:8px;padding:12px}
.card h2{font-size:14px;margin:0 0 8px}
.wide{flex:1 1 700px;min-width:320px}
#map{height:540px;width:100%;border-radius:6px;background:#e8e8e8}
canvas{display:block}
table{border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:4px 10px;border-bottom:1px solid #eee}
.sw{display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:6px;vertical-align:middle}
.note{color:#777;font-size:12px;margin-top:8px;max-width:560px}
.scroll{max-height:540px;overflow:auto}
.legend{background:rgba(255,255,255,.92);padding:8px 10px;border-radius:6px;font-size:13px;line-height:1.5;box-shadow:0 1px 4px rgba(0,0,0,.3);max-height:300px;overflow:auto}
.legend small{color:#777}
.console{background:#111;color:#d6f5e3;font:12px/1.55 ui-monospace,Menlo,monospace;padding:8px;border-radius:6px;height:280px;overflow:auto;white-space:pre;word-break:normal}
.bar{display:flex;align-items:center;margin:3px 0;font-size:13px}
.bar span.l{width:110px} .bar small{color:#888}
.bar div{height:14px;margin-right:6px;border-radius:2px}
button{font-size:12px;padding:3px 8px;margin-right:8px}
</style></head><body>
<h1>Fleet telemetry live</h1>
<div id="stats">waiting for data...</div>
<div class="row">
  <div class="card wide"><h2>Live map: vehicles, their recent paths, and official routes</h2>
    <div id="map"></div>
    <div class="note"><button id="fit">Fit map to vehicles</button><span id="mapnote"></span><br>
    Dots are the latest position of each vehicle. Lines of the same color behind a dot are its recent path.
    Faint long lines are the official routes. Use the box in the map's corner to turn layers on or off.
    Larger dots are autonomous vehicles.</div></div>
  <div class="card" style="flex:0 1 560px"><h2>Who is reporting</h2>
    <div class="scroll"><table id="tbl"></table></div>
    <div class="note">Transit agencies come from the 511 feeds. Waymo and Zoox have no public live feed, so they
    appear only if a recorded file is replayed. Simulated data is marked as such. "Data age" is how old a report
    was when our producer received it.</div>
  </div>
</div>
<div class="row">
  <div class="card"><h2>Trip delays (BART and other trip-update feeds)</h2>
    <table id="dtbl"></table>
    <div class="note">BART publishes delays but no vehicle positions, so it appears here and not on the map.</div></div>
  <div class="card"><h2>Events per route (top 15)</h2><div id="bars"></div></div>
</div>
<div class="row">
  <div class="card"><h2>Events per 10 seconds, last 10 minutes (top 10 sources)</h2>
    <canvas id="tl" width="900" height="260"></canvas>
    <div id="legend" class="note"></div></div>
</div>
<div class="row">
  <div class="card" style="flex:1 1 100%">
    <h2>Live Kafka messages (what the console consumer shows)</h2>
    <div style="margin-bottom:6px;font-size:13px">
      <label><input type="checkbox" id="cpause"> Pause</label>
      &nbsp; Filter: <input id="cfilter" placeholder="e.g. SC, VTA, a vehicle id, a route" style="width:260px;padding:3px">
      <span id="ccount" style="color:#777;margin-left:10px"></span></div>
    <div id="console" class="console">waiting for messages...</div>
    <div class="note">Each line is one message exactly as it is in Kafka: time received, topic, key (the vehicle id), then the JSON value.
    Only real messages from the live topics appear here.</div>
  </div>
</div>
<script>
const FIXED={WAYMO:"#00a6a6",ZOOX:"#ff7a00",BA:"#3b7dd8",SIM:"#7f8c8d"};
const colorMap={}; let colorN=0;
function colorOf(k){
  if(FIXED[k]) return FIXED[k];
  if(!colorMap[k]) colorMap[k]="hsl("+((colorN++)*137.508%360)+",65%,42%)";   // each new source gets a clearly different hue
  return colorMap[k];
}
const tl=document.getElementById("tl"), tctx=tl.getContext("2d");

const HAVE_MAP = (typeof L !== "undefined");
let map, routeLayer, pathLayer, vehLayer, fitted=false, lastPts=[];
if(HAVE_MAP){
  map=L.map("map",{preferCanvas:true}).setView([37.77,-122.42],11);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    {maxZoom:19,attribution:"&copy; OpenStreetMap contributors"}).addTo(map);
  routeLayer=L.layerGroup().addTo(map); pathLayer=L.layerGroup().addTo(map); vehLayer=L.layerGroup().addTo(map);
  L.control.layers(null,{"Official routes":routeLayer,"Vehicle paths":pathLayer,"Vehicles":vehLayer},{collapsed:false}).addTo(map);
  window.dash={map,routeLayer,pathLayer,vehLayer};
  const LG=L.control({position:"bottomleft"});
  LG.onAdd=function(){const d=L.DomUtil.create("div","legend");d.innerHTML="Waiting for vehicles...";return d;};
  LG.addTo(map); window.legendEl=LG.getContainer();
  document.getElementById("fit").onclick=()=>{if(lastPts.length)map.fitBounds(L.latLngBounds(lastPts).pad(0.1));};
}else{
  document.getElementById("map").innerHTML="<p style='padding:20px'>The map library could not load. It needs an internet connection (it loads from unpkg.com). The rest of the dashboard still works.</p>";
}

const GLIDE_MS=3000, vehMarkers={}, pathLines={};
function glide(m,to){
  const from=m.getLatLng();
  if(from.lat===to[0]&&from.lng===to[1]) return;
  if(m._raf) cancelAnimationFrame(m._raf);
  let t0=null;
  function step(t){
    if(t0===null) t0=t;
    const k=Math.min(1,(t-t0)/GLIDE_MS);
    m.setLatLng([from.lat+(to[0]-from.lat)*k, from.lng+(to[1]-from.lng)*k]);
    if(k<1) m._raf=requestAnimationFrame(step);
  }
  m._raf=requestAnimationFrame(step);
}
function drawMap(d){
  if(!HAVE_MAP) return;
  const all=[], seen={}, cnt={};
  d.trails.forEach(t=>{
    const key=t.src+"|"+t.id, c=colorOf(t.src), av=d.kinds[t.src]==="Autonomous";
    const ll=t.pts.map(p=>[p[1],p[0]]), last=ll[ll.length-1];
    seen[key]=1; all.push(last); cnt[t.src]=(cnt[t.src]||0)+1;
    const tip="<b>"+(d.names[t.src]||t.src)+"</b><br>route "+(t.route||"?")+" - vehicle "+t.id;
    let m=vehMarkers[key];
    if(!m){
      m=L.circleMarker(last,{radius:av?8:4,color:"#fff",weight:1,fillColor:c,fillOpacity:0.95}).bindTooltip(tip).addTo(vehLayer);
      vehMarkers[key]=m;
    }else{ m.setTooltipContent(tip); glide(m,last); }
    let pl=pathLines[key];
    if(ll.length>1){
      if(!pl){ pl=L.polyline(ll,{color:c,weight:av?4:2,opacity:0.65}).addTo(pathLayer); pathLines[key]=pl; }
      else pl.setLatLngs(ll);
    }
  });
  Object.keys(vehMarkers).forEach(k=>{ if(!seen[k]){ vehLayer.removeLayer(vehMarkers[k]); delete vehMarkers[k];
    if(pathLines[k]){ pathLayer.removeLayer(pathLines[k]); delete pathLines[k]; } } });
  window.legendEl.innerHTML="<b>Who is who</b>"+Object.keys(cnt).sort((a,b)=>cnt[b]-cnt[a]).map(k=>
    "<div><span class='sw' style='background:"+colorOf(k)+"'></span>"+(d.names[k]||k)+" <small>("+cnt[k]+")</small></div>").join("");
  lastPts=all;
  if(!fitted && all.length){ map.fitBounds(L.latLngBounds(all).pad(0.1)); fitted=true; }
}

async function loadShapes(){
  if(!HAVE_MAP) return;
  try{
    const s=await (await fetch("/shapes")).json();
    routeLayer.clearLayers(); let n=0;
    Object.keys(s).forEach(a=>s[a].forEach(r=>{
      L.polyline(r.pts,{color:colorOf(a),weight:2,opacity:0.4}).bindTooltip(((window.__names&&window.__names[a])||a)+" route "+r.route).addTo(routeLayer); n++;}));
    document.getElementById("mapnote").textContent = n
      ? n+" official route lines loaded. "
      : "No official route lines yet. Run ingestion/fetch_routes.py to add them. ";
  }catch(e){}
}

function drawTimeline(d){
  tctx.clearRect(0,0,tl.width,tl.height);
  const tot=k=>d.timeline[k].reduce((a,b)=>a+b,0);
  const keys=Object.keys(d.timeline).sort((a,b)=>tot(b)-tot(a)).slice(0,10); const L_=60;
  let max=1; keys.forEach(k=>d.timeline[k].forEach(v=>{if(v>max)max=v;}));
  const padL=40,padB=22,padT=10,padR=10,w=tl.width-padL-padR,h=tl.height-padT-padB;
  tctx.font="11px sans-serif";
  for(let i=0;i<=4;i++){
    const y=padT+h-i*h/4;
    tctx.strokeStyle="#e5e5e5"; tctx.beginPath(); tctx.moveTo(padL,y); tctx.lineTo(padL+w,y); tctx.stroke();
    tctx.fillStyle="#666"; tctx.fillText(String(Math.round(max*i/4)),4,y+4);
  }
  tctx.fillStyle="#666"; tctx.fillText("10 min ago",padL,tl.height-6); tctx.fillText("now",padL+w-20,tl.height-6);
  keys.forEach(k=>{
    tctx.strokeStyle=colorOf(k); tctx.lineWidth=2; tctx.beginPath();
    d.timeline[k].forEach((v,i)=>{const x=padL+i*w/(L_-1), y=padT+h-v/max*h; i?tctx.lineTo(x,y):tctx.moveTo(x,y);});
    tctx.stroke();
  });
  tctx.lineWidth=1;
  document.getElementById("legend").innerHTML=keys.map(k=>
    "<span class='sw' style='background:"+colorOf(k)+"'></span>"+(d.names[k]||k)).join(" &nbsp; ");
}

function drawConsole(d){
  if(document.getElementById("cpause").checked) return;
  const f=document.getElementById("cfilter").value.trim().toLowerCase();
  const box=document.getElementById("console");
  const rows=d.messages.slice().reverse().filter(m=>!f||(m.topic+" "+m.key+" "+JSON.stringify(m.value)).toLowerCase().indexOf(f)>=0).slice(0,60);
  document.getElementById("ccount").textContent=rows.length+" of the last "+d.messages.length+" messages";
  box.textContent="";
  if(!rows.length){ box.textContent=d.messages.length?"no messages match the filter":"waiting for messages..."; return; }
  rows.forEach(m=>{
    const line=document.createElement("div");
    const dot=document.createElement("span"); dot.className="sw"; dot.style.background=colorOf(m.value.source||"?");
    line.appendChild(dot);
    line.appendChild(document.createTextNode(new Date(m.t*1000).toLocaleTimeString()+"  "+m.topic+"  "+m.key+" | "+JSON.stringify(m.value)));
    box.appendChild(line);
  });
}
async function tick(){
  try{
    const d=await (await fetch("/data")).json();
    window.__names=d.names;
    const rank={Autonomous:0,Transit:1,Simulated:2,"Replay (not live)":3};
    const keys=Object.keys(d.sources).sort((a,b)=>
      (rank[d.kinds[a]]-rank[d.kinds[b]])||(d.sources[b].vehicles-d.sources[a].vehicles));
    const nveh=keys.reduce((a,k)=>a+d.sources[k].vehicles,0);
    const nev=keys.reduce((a,k)=>a+d.sources[k].events,0);
    const nd=Object.keys(d.delays).length;
    document.getElementById("stats").textContent=nev+" position events, "+nveh+" vehicles, "+keys.length+
      " sources"+(nd?", plus trip delays from "+nd+" source"+(nd>1?"s":""):"");
    const newest=Math.max(0,...keys.map(k=>d.sources[k].last||0));
    if(newest){ const ago=Math.max(0,Math.round(Date.now()/1000-newest));
      document.getElementById("stats").textContent+=" | newest report "+(ago<120?ago+" s":Math.round(ago/60)+" min")+" ago (511 sends new positions about once a minute per agency)"; }
    const notLive=keys.filter(k=>d.kinds[k]!=="Transit");
    document.getElementById("stats").textContent+=notLive.length?
      " | NOT LIVE: includes "+notLive.map(k=>d.names[k]||k).join(", "):" | live data only";
    if(!keys.length&&!nd) document.getElementById("stats").textContent="No real data yet. Start ingestion/producer_all.py (transit) or ingestion/producer_bart.py (BART delays).";

    const now=Date.now()/1000;
    document.getElementById("tbl").innerHTML=
      "<tr><th>Source</th><th>Type</th><th>Vehicles</th><th>Events</th><th>Data age</th><th>Last report</th></tr>"+
      keys.map(k=>{const s=d.sources[k];const ago=s.last?Math.max(0,Math.round(now-s.last)):null;
        return "<tr><td><span class='sw' style='background:"+colorOf(k)+"'></span>"+(d.names[k]||k)+" ("+k+")</td><td>"+
          d.kinds[k]+"</td><td>"+s.vehicles+"</td><td>"+s.events+"</td><td>"+
          (s.age===null?"-":Math.round(s.age)+" s")+"</td><td>"+
          (ago===null?"-":(ago<120?ago+" s ago":Math.round(ago/60)+" min ago"))+"</td></tr>";}).join("");

    const dk=Object.keys(d.delays);
    document.getElementById("dtbl").innerHTML=dk.length?
      "<tr><th>Source</th><th>Delay records</th><th>Trips</th><th>Avg delay</th><th>Worst delay</th></tr>"+
      dk.map(k=>{const v=d.delays[k];
        return "<tr><td><span class='sw' style='background:"+colorOf(k)+"'></span>"+(d.names[k]||k)+" ("+k+")</td><td>"+
          v.n+"</td><td>"+v.trips+"</td><td>"+Math.round(v.avg)+" s</td><td>"+v.max+" s</td></tr>";}).join(""):
      "<tr><td>No trip-delay data yet. Run ingestion/producer_bart.py.</td></tr>";

    drawMap(d);
    const top=d.routes; const mx=top.length?top[0].n:1;
    document.getElementById("bars").innerHTML=top.map(r=>
      '<div class="bar"><span class="l">'+r.route+' <small>'+r.src+'</small></span><div style="background:'+
      colorOf(r.src)+';width:'+Math.max(2,r.n/mx*220)+'px"></div>'+r.n+'</div>').join("");
    drawTimeline(d);
    drawConsole(d);
  }catch(e){document.getElementById("stats").textContent="waiting for server...";}
}
loadShapes(); setInterval(loadShapes,60000);
tick(); setInterval(tick,2000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data":
            body, ctype = json.dumps(snapshot()).encode(), "application/json"
        elif self.path == "/shapes":
            body, ctype = shapes_json(), "application/json"
        else:
            body, ctype = PAGE.encode(), "text/html"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


threading.Thread(target=consume, daemon=True).start()
print("Dashboard running: open http://localhost:8000  (Ctrl+C to stop)")
ThreadingHTTPServer(("127.0.0.1", 8000), Handler).serve_forever()
