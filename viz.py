import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("data/parquet/vehicle_events")
print(f"{len(df)} events, {df['vehicle_id'].nunique()} vehicles, "
      f"{df['route_id'].nunique()} routes")
print(df[["vehicle_id", "route_id", "lat", "lon", "event_time"]].head())

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Left: where each vehicle moved
for vid, g in df.sort_values("event_time").groupby("vehicle_id"):
    axes[0].plot(g["lon"], g["lat"], marker=".", linewidth=0.8, label=vid)
axes[0].set_title("Vehicle paths")
axes[0].set_xlabel("Longitude")
axes[0].set_ylabel("Latitude")

# Right: events per route
counts = df.groupby("route_id").size().sort_values(ascending=False)
axes[1].bar(counts.index.astype(str), counts.values)
axes[1].set_title("Events per route")
axes[1].set_xlabel("Route")
axes[1].set_ylabel("Events")

plt.tight_layout()
plt.savefig("fleet_overview.png", dpi=150)
print("Saved fleet_overview.png")
plt.show()
