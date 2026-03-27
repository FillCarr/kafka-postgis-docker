# GPS Sensor Streaming Dashboard

A real-time data pipeline and GUI dashboard built on [comma.ai's public dataset](https://github.com/commaai/comma2k19) as a exercise in data processing and visualisation. Ingests GPS sensor data via Apache Kafka, persists it to a PostGIS database, and visualises it live using Python/Tkinter.

<img width="1917" height="985" alt="image" src="https://github.com/user-attachments/assets/5121d339-0543-47d1-8f6b-c17fd06487cc" />

## Architecture
```
Producer (CSV) --> Kafka --> Consumer --> PostGIS (Docker)
                                    └──> Tkinter GUI (live charts + map + table)
```

## Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka (kafka-python) |
| Database | PostgreSQL + PostGIS (Docker) |
| GUI | Tkinter + tkintermapview |
| Charts | Matplotlib (stairs plot, ECDF) |
| Language | Python 3.11 |

## Prerequisites

- Python 3.11
- Docker Desktop
- Apache Kafka running locally on `localhost:9092` — [Kafka quickstart](https://kafka.apache.org/quickstart)
- comma.ai dataset (~100GB) — [download here](https://github.com/commaai/comma2k19)

## Setup

**1. Start the database**
```bash
docker compose up -d
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Follow the instructions in the commented sections** of both `Consumer.py` and `producer.py` to configure paths and connection strings before running.

**4. To simulate live data, start the consumer first, then the producer**
```bash
python Consumer.py
```
```bash
python producer.py
```

> Run `Consumer.py` before `producer.py` if you want to simulate a live data stream.

## Database Schema
```sql
CREATE TABLE kafka (
    id      serial PRIMARY KEY,
    speed   float,
    lat     float,
    lon     float,
    sats    integer,
    dis_m   float,
    lap_t   float,
    elev    float
);
```

## Notes

- The Docker Compose file only defines PostGIS. Kafka must be running separately.
- Database credentials are set in `compose.yaml`. Update `Consumer.py` and `producer.py` connection strings if changed.

---

That's all folks — thanks for checking it out.
