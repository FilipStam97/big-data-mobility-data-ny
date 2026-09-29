# Big Data Systems – Project 2
## Big Mobility Data Stream Analytics

Stream-processing project for analyzing vehicle movement, traffic, and emissions in **Midtown Manhattan, New York City**.

The project uses:

- **SUMO** – traffic simulation and mobility/emission dataset generation
- **Apache Kafka** – streaming transport
- **Apache Spark Structured Streaming** – stream-processing implementation
- **Apache Flink / PyFlink** – second stream-processing implementation
- **Folium** – visualization of analytics results
- **Docker Compose** – local Kafka, Spark, and Flink clusters

---

## Architecture

```text
                         SUMO
                  FCD + Emissions XML
                          |
                          v
                  Python Producer
                          |
                          v
                 Kafka: vehicle-data
                    /           \
                   /             \
                  v               v
        Spark Structured       Apache Flink
           Streaming             PyFlink
                  \               /
                   \             /
                    v           v
              Kafka: analytics-results
                          |
                          v
                  Python Consumer
                          |
                          v
                     Folium Map
```

---

## Dataset

The SUMO simulation represents vehicle movement in **Midtown Manhattan, New York City**.

Two SUMO outputs are used:

- FCD output – vehicle position, speed, lane, etc.
- Emission output – CO2, CO, HC, NOx, PMx and noise

Final generated dataset:

| File | Size |
|---|---:|
| `fcd.final.xml` | 403.98 MB |
| `emissions.final.xml` | 800.35 MB |
| **Total** | **~1.20 GB** |

FCD and emission records are joined by simulation timestamp and vehicle ID before being sent to Kafka.

---

## Spatial Entities

Analytics are calculated for vehicles within a **200 m radius** of:

- Times Square
- Empire State Building
- Grand Central Terminal
- Bryant Park

For every time window the applications calculate:

- number of vehicles
- average speed
- average CO2
- average CO
- average HC
- average NOx
- average PMx
- average noise

---

# Running the Project

## 1. Start Docker services

From the project root:

```powershell
docker compose up -d
```

Check containers:

```powershell
docker ps
```

Spark UI:

```text
http://localhost:8081
```

Flink UI:

```text
http://localhost:8082
```

---

## 2. Kafka Topics

List topics:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --list
```

Create input topic:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create `
  --topic vehicle-data `
  --partitions 4 `
  --replication-factor 1
```

Create analytics output topic:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create `
  --topic analytics-results `
  --partitions 1 `
  --replication-factor 1
```

Check topic configuration:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --describe `
  --topic vehicle-data
```

---

## Reset Kafka Topics

Useful before benchmarks or clean tests:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --delete --topic vehicle-data

docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --delete --topic analytics-results
```

Then recreate them:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create --topic vehicle-data `
  --partitions 4 `
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create --topic analytics-results `
  --partitions 1 `
  --replication-factor 1
```

---

# Producer

Install dependency if needed:

```powershell
pip install kafka-python
```

Run as fast as possible:

```powershell
python producer/producer.py --delay 0
```

Simulate a slower real-time stream:

```powershell
python producer/producer.py --delay 0.1
```

Send only 100,000 records:

```powershell
python producer/producer.py --delay 0 --limit 100000
```

Time a producer run:

```powershell
$sw = [System.Diagnostics.Stopwatch]::StartNew()

python producer/producer.py --delay 0 --limit 100000

$sw.Stop()
Write-Host "Producer seconds:" $sw.Elapsed.TotalSeconds
```

---

# Spark Structured Streaming

## Run Spark application

```powershell
docker exec -it p2-spark-app `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 `
  /app/streaming.py `
  --window 60 `
  --slide 60 `
  --attributes speed,co2,co,hc,nox,pmx,noise
```

### Tumbling window

A 60-second tumbling window:

```text
--window 60 --slide 60
```

### Sliding window

A 60-second window sliding every 30 seconds:

```text
--window 60 --slide 30
```

### Select attributes

Example:

```text
--attributes speed,co2,noise
```

Available attributes:

```text
speed
co2
co
hc
nox
pmx
noise
```

---

## Reset Spark Checkpoint

When performing a clean test or after changing the streaming aggregation:

```powershell
docker exec p2-spark-app `
  rm -rf /tmp/checkpoints/spark-analytics
```

This should also be done before a clean benchmark.

---

# Apache Flink

Run the PyFlink application:

```powershell
docker exec -it p2-flink-jobmanager `
  flink run `
  -py /app/streaming.py `
  --window 60 `
  --slide 60 `
  --attributes speed,co2,co,hc,nox,pmx,noise
```

Flink UI:

```text
http://localhost:8082
```

### Tumbling window

```text
--window 60 --slide 60
```

### Sliding window

```text
--window 60 --slide 30
```

### Selected attributes example

```text
--attributes speed,co2,noise
```

---

# Consumer / Visualization

Run the analytics consumer:

```powershell
python consumer/consumer.py
```

The consumer reads analytics results from:

```text
analytics-results
```

and generates a **Folium map** containing the selected New York locations and their latest Spark/Flink analytics.

---

# Performance Test

Spark and Flink were tested locally using Docker clusters.

Both frameworks used the same:

- Kafka input
- SUMO mobility data
- four spatial locations
- 200 m search radius
- 60-second tumbling windows
- seven analytics attributes

A controlled stream was used for local performance testing.

## Cluster configuration

### Spark

```text
2 Spark workers
2 cores per worker
1 GB executor memory per worker
```

### Flink

```text
2 TaskManagers
2 slots per TaskManager
2 GB TaskManager process size
```

## Observed Performance

| Metric | Spark | Flink |
|---|---:|---:|
| Test stream | 100k-record runs | 100k-record runs |
| Window | 60 s tumbling | 60 s tumbling |
| Attributes | 7 | 7 |
| Parallelism | 4 cores | 4 slots |
| Observed processing/input rate | ~6.7k records/s | ~1.5k records/s |
| Peak observed processing rate | ~7.5k records/s | — |

The measurements were obtained from each framework's native runtime metrics and therefore represent an **indicative local comparison rather than identical low-level throughput measurements**.

Producer execution time was not used as the primary framework-performance metric because XML parsing, JSON serialization, Kafka transmission, and local machine conditions also affect producer throughput.

---

# Clean Benchmark Procedure

### 1. Stop existing Spark/Flink jobs.

### 2. Reset Kafka topics.

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --delete --topic vehicle-data

docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --delete --topic analytics-results
```

Recreate:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create --topic vehicle-data `
  --partitions 4 `
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --create --topic analytics-results `
  --partitions 1 `
  --replication-factor 1
```

### 3. For Spark, reset checkpoint.

```powershell
docker exec p2-spark-app `
  rm -rf /tmp/checkpoints/spark-analytics
```

### 4. Start either Spark or Flink.

### 5. Send test records.

```powershell
python producer/producer.py --delay 0 --limit 100000
```

### 6. Record processing metrics.

Spark reports `processedRowsPerSecond`.

Flink metrics can be inspected from:

```text
http://localhost:8082
```

Useful Flink metrics include:

```text
numRecordsIn
numRecordsInPerSecond
numRecordsOut
numRecordsOutPerSecond
```

---

# Useful Docker Commands

Check containers:

```powershell
docker ps
```

Check resource usage:

```powershell
docker stats
```

Restart services:

```powershell
docker compose restart
```

Stop project:

```powershell
docker compose down
```

Rebuild after changing the Flink Docker image:

```powershell
docker compose up -d --build
```

Remove orphaned containers:

```powershell
docker compose up -d --remove-orphans
```

View Kafka logs:

```powershell
docker logs kafka
```

View Spark master logs:

```powershell
docker logs p2-spark-master
```

View Flink JobManager logs:

```powershell
docker logs p2-flink-jobmanager
```

---

# Project Structure

```text
big-data-project-2/
├── data/
│   ├── fcd.xml
│   └── emissions.xml
├── producer/
│   └── producer.py
├── spark/
│   └── streaming.py
├── flink/
│   ├── streaming.py
│   ├── Dockerfile
│   └── pom.xml
├── consumer/
│   └── consumer.py
├── config/
├── docker-compose.yml
└── README.md
```

Large SUMO XML datasets should not be committed to GitHub.

---

## Technologies

- Python
- SUMO
- Apache Kafka 4.1.1
- Apache Spark 4.0.2
- Apache Flink 2.1.0
- PyFlink
- Docker / Docker Compose
- Folium