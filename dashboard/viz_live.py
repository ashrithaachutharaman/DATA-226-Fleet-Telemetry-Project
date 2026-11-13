import os
from dotenv import load_dotenv

load_dotenv()
import json
import time
import uuid
from collections import defaultdict, deque

import matplotlib.pyplot as plt
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "group.id": f"viz-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",   # also shows events already sent
})
consumer.subscribe(["vehicle-positions"])

trails = defaultdict(lambda: deque(maxlen=30))   # last 30 points per vehicle
route_counts = defaultdict(int)
total = 0

plt.ion()
fig, (ax_map, ax_bar) = plt.subplots(1, 2, figsize=(13, 5))
last_draw = 0

try:
    while plt.fignum_exists(fig.number):
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            e = json.loads(msg.value())
            if e.get("lat") is not None and e.get("lon") is not None:
                trails[e["vehicle_id"]].append((e["lon"], e["lat"]))
                route_counts[e.get("route_id") or "unknown"] += 1
                total += 1

        if time.time() - last_draw > 2:
            last_draw = time.time()
            ax_map.clear()
            for vid, pts in trails.items():
                xs, ys = zip(*pts)
                ax_map.plot(xs, ys, linewidth=0.8, alpha=0.6)
                ax_map.scatter(xs[-1], ys[-1], s=25)
            ax_map.set_title(f"Vehicle positions (last 30 per vehicle) - {len(trails)} vehicles")
            ax_map.set_xlabel("Longitude")
            ax_map.set_ylabel("Latitude")

            ax_bar.clear()
            top = sorted(route_counts.items(), key=lambda kv: -kv[1])[:15]
            if top:
                ax_bar.bar([str(k) for k, _ in top], [v for _, v in top])
            ax_bar.set_title(f"Events per route (top 15) - {total} events total")
            ax_bar.set_xlabel("Route")
            ax_bar.set_ylabel("Events")
            ax_bar.tick_params(axis="x", rotation=45)

            fig.tight_layout()
            plt.pause(0.01)
finally:
    consumer.close()
