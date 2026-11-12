# Real-Time Fleet Telemetry Pipeline: From Bay Area Transit to Autonomous Vehicles

DATA 226 term project · Ashritha Achutharaman, Peter Harlan, Jason Letargo

A streaming pipeline that ingests live GTFS-realtime feeds (511, BART, VTA) through Apache Kafka, processes them with Spark Structured Streaming, detects rare operational events (bus bunching, delay propagation, stalled vehicles), and benchmarks how design choices trade storage and latency against detection recall, measured against exact offline ground truth.

## Status

| Sprint | Dates | Goal | Status |
| --- | --- | --- | --- |
| 1 | Oct 19 – Oct 25 | Setup + live GTFS-rt data in Kafka | In progress |
| 2 | Oct 26 – Nov 1 | Spark stream, storage layers, intermediate report | Not started |
| 3 | Nov 2 – Nov 8 | Detectors, Bloom filter, LSH | Not started |
| 4 | Nov 9 – Nov 15 | Benchmarks, privacy, streaming algorithms, slides | Not started |
| Wrap-up | Nov 16 – Nov 17 | Final report and presentation | Not started |

## Architecture

```
GTFS-realtime feeds (511 / BART / VTA)
        │  Python producers (ingestion/)
        ▼
   Apache Kafka  ──►  Spark Structured Streaming (streaming/)
                          │  windowed features, detectors, Bloom filter dedup
                          ├──►  Raw-event store: MongoDB
                          ├──►  Warehouse: ClickHouse or DuckDB (alerts, summaries)
                          └──►  Parquet archive (data/, not committed)
```

A full flow diagram will be added in `docs/`.

## Repository layout

| Folder | Contents |
| --- | --- |
| `ingestion/` | Kafka producers for GTFS-realtime feeds and replay |
| `streaming/` | Spark Structured Streaming jobs and detectors |
| `storage/` | MongoDB and warehouse schemas, sink code, sample queries |
| `algorithms/` | Bloom filter, LSH, reservoir sampling, Flajolet-Martin, privacy |
| `benchmarks/` | Benchmark harness, fault injection, results CSVs |
| `notebooks/` | Exploration and analysis notebooks (each also exported to `.py`) |
| `docs/` | Diagrams, meeting minutes, evidence for the rubric appendix |
| `report/` | LaTeX source of the IEEE report (synced from Overleaf) |
| `data/` | Local data only; ignored by Git |

## Setup

1. Install Docker Desktop and Python 3.10+.
2. Clone the repo and create a virtual environment:
   ```
   git clone https://github.com/<your-account>/fleet-telemetry-pipeline.git
   cd fleet-telemetry-pipeline
   python -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and add your 511.org API key.
4. Start Kafka and Spark: `docker compose up -d` (docker-compose.yml added in backlog item B2).

## Project links
- https://github.com/peterharlan7/DATA-226-Fleet-Telemetry-Project/tree/main
- Trello board (sprints and backlog): <add link>
- Overleaf report: <add link>
- Elevator pitch video: <add link>

## Team

| Member | Role |
| --- | --- |
| Ashritha Achutharaman | Ingestion and Kafka lead; Product Owner |
| Peter Harlan | Spark and algorithms lead; Scrum Master (odd sprints) |
| Jason Letargo | Storage, benchmarking and privacy lead; Scrum Master (even sprints) |
