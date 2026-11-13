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

## Common event schema (read this before writing a producer or Spark job) *********

        Every record in the pipeline follows one schema, defined once in
        [`ingestion/schema.py`](ingestion/schema.py) (card B4). Producers, Spark jobs,
        detectors and privacy code all import it. Do not redefine fields elsewhere.

**Two record types**
        
        | Record | Kafka topic | Used for |
        | --- | --- | --- |
        | Vehicle event (`VEHICLE_EVENT_FIELDS`) | `vehicle-positions` | Archive, bus bunching, stalled vehicles |
        | Trip-update event (`TRIP_UPDATE_EVENT_FIELDS`) | `trip-updates` | Delay propagation |
        
**Rules**
        
        - **Timestamps are epoch milliseconds, UTC.** GTFS-realtime sends seconds, so multiply by 1000.
        - Two timestamps: `event_ts` (when the vehicle reported, used for windows) and `ingest_ts` (when our producer received it).
        - Units: meters, meters per second (`speed_mps`), degrees.
        - `source` uses 511 operator codes (`SC` = VTA, `SF` = Muni, `BA` = BART); test data uses `fake`.
        - Build `event_id` with `make_vehicle_event_id()` / `make_trip_update_event_id()`.
        - Call `validate()` before publishing a record.
        
**How to use it**
        
        ```python
        from ingestion.schema import VEHICLE_EVENT_FIELDS, validate, spark_schema
        
        problems = validate(record, VEHICLE_EVENT_FIELDS)    # [] means valid
        schema = spark_schema(VEHICLE_EVENT_FIELDS)          # Spark StructType for from_json
        ```
        
        Run `python ingestion/schema.py` to print an example record and the Spark schema.

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

## Running the producers

All events follow the common schema in `ingestion/schema.py` (timestamps are epoch milliseconds).
Run these from the repo root with Kafka up (`docker compose up -d`) and a key in `.env`:

| Script | What it is | Run it |
| --- | --- | --- |
| `ingestion/producer_all.py` | **Main producer.** Live vehicle positions from 511, one request per agency, so `source` is the agency code (SF, SC, AC, ...) | `python ingestion/producer_all.py --agencies SF,SC,AC` (use `--list` to see all 511 codes) |
| `ingestion/producer_bart.py` | BART trip delays (BART has no vehicle-position feed) | `python ingestion/producer_bart.py` |
| `ingestion/producer_vta.py` | One-feed VTA producer used for the Trello card demo | `python ingestion/producer_vta.py --once` |
| `ingestion/producer_511.py` | Shared functions only (not a program) | imported by the scripts above |
| `ingestion/av_replay.py`, `record_stream.py` | Replay and recording tools for the benchmarks; replays go to `replay-*` topics and are tagged with a Kafka header | see the docstring in each file |
| `ingestion/simulator_producer.py` | Simulated data (source `SIM`), testing only | `python ingestion/simulator_producer.py` |

Vehicle ids are hashed at ingestion (B13, `ingestion/hashing.py`) and `event_id` is built from the hashed id, so raw ids never reach Kafka.
The producers need `VEHICLE_ID_SALT` in `.env` (ask Jason; it must be the same for everyone) and stop at start-up if it is missing.

**Live dashboard: [http://localhost:8000](http://localhost:8000)** (real Kafka data, updates every 2 seconds). Start it with `./demo.sh`, or run `python dashboard/viz_web.py` after the producers are running. A screenshot of a real run is in `docs/dashboard.png`.

Docker Compose also starts MongoDB (`mongo`, port 27017) for the raw-event store (B11). Its address is `MONGO_URI` in `.env`.
Checks: `./producer_check.sh SC` (producer to Kafka), `python ingestion/check_real.py` (only real data, follows the schema), `./spark_check.sh` (Kafka to Spark to Parquet).
