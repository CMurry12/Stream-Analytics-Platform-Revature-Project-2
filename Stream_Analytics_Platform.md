# Project Spec: Stream Analytics Platform

## 1. Summary

An end-to-end data pipeline system that ingests real-time event streams via **Apache Kafka**, processes and transforms them using **PySpark DataFrames/SparkSQL**, and orchestrates the entire workflow via **Apache Airflow** (optional - all running in **Docker containers**.)

---

## 2. Architecture Pattern

>**NOTE: Micro-Batch via File Buffer** - This project uses batch-style Airflow orchestration with Kafka as the event source. Continuous Spark Structured Streaming is NOT compatible with Airflow task execution.

### Data Flow
```
Producers -> Kafka Topics -> Batch Consumer -> Landing Zone (JSON)
                                                              |
                                                              v
                                     Gold Zone (CSV) <- Spark ETL
```

### Why This Pattern?
- **Airflow** is a batch orchestrator - it expects tasks to *start* and *finish*
- **Structured Streaming** jobs run indefinitely and would hang Airflow tasks
- **Solution**: A Python consumer script creates bounded JSON files (the "buffer"), which Spark then processes as batch jobs

---

## 3. Local Infrastructure Architecture

All components run locally using Docker.

### Docker Compose (optional) Stack 
A single `docker-compose.yml` provisions the entire platform:

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `kafka` | bitnami/kafka:3.6 | 9092, 9094 | Message broker (KRaft mode) |
| `kafka-ui` | provectuslabs/kafka-ui | 8080 | Kafka monitoring dashboard |
| `spark-master` | bitnami/spark:3.5 | 7077, 8081 | Spark cluster manager |
| `spark-worker` | bitnami/spark:3.5 | - | Spark executor |
| `airflow` | Custom (Dockerfile.airflow) | 8082 | Workflow orchestration |

> The Airflow service uses a **custom Dockerfile** that extends the official image with JDK 17 and Spark client binaries. This enables `spark-submit` from `BashOperator` tasks.

### Volume Mounts
| Local Path | Container Path | Purpose |
|------------|----------------|---------|
| `./jobs` | `/opt/spark-jobs` | PySpark job scripts |
| `./data` | `/opt/spark-data` | Input/output datasets |
| `./dags` | `/opt/airflow/dags` | Airflow DAG definitions |
| `./logs` | `/opt/airflow/logs` | Airflow execution logs |

---

## 4. Functional Modules

### Module A: The Streaming Ingestion Layer (Kafka)

- **Objective:** Architect a fault-tolerant event streaming system to ingest raw data from multiple sources.

- **Key Deliverables:**
    - Kafka (optional: running in Docker) with distinct topics:
        - `user_events` - ex :User activity data (logins, page views, clicks).
        - `transaction_events` - ex: E-commerce/financial transaction data.
    - **Batch Consumer Script:** A Python script that consumes from Kafka for a configurable time window and writes bounded JSON files to the landing zone.
    - Documentation justifying the topic schema and partitioning strategy.

- **Provided Assets: (for the example setup) **
    - `user_events_producer.py` - Generates mock user activity events.
    - `transaction_events_producer.py` - Generates mock transaction events.
    - `jobs/ingest_kafka_to_landing.py` - **Skeleton starter** for the batch consumer.

---

### Module B: The PySpark Transformation Engine (optional Docker Spark Cluster)

- **Objective:** Build a high-performance batch processing layer using DataFrames and SparkSQL.

- **Key Deliverables:**
    - **SparkSession Factory Module:**
        - Cluster-aware configuration connecting to `spark://spark-master:7077`.
        - Parameterized initialization supporting development and production settings.
    - **Multi-Stage DataFrame Transformation Pipeline:**
        - Complex Joins: inner, left, anti-join across event streams.
        - Aggregations with Window Functions: ranking, running totals, moving averages.
        - Set Operations: union, intersect, except.
        - Dynamic column manipulation: add, remove, rename, cast.
        - JSON dataset parsing and semi-structured data flattening.
    - **Performance Optimization:**
        - Partitioning and bucketing strategies for output datasets.
        - Caching policies for intermediate DataFrames.
    - **Parameterization:**
        - The pipeline must support parameterized runs (dates, input paths, output paths).
        - Must execute via `spark-submit` without manual intervention.

- **Provided Assets:**
    - `jobs/spark_session_factory.py` - **Skeleton starter** for session factory.
    - `jobs/etl_job.py` - **Skeleton starter** for transformation pipeline.

---

### Module C: The Orchestration Control Plane (Airflow)

- **Objective:** Design a robust DAG-based orchestration layer that schedules, monitors, and manages the entire pipeline.

- **Key Deliverables:**
    - **Dynamic, Parameterized DAG:**
        - Kafka consumer trigger task (runs the batch ingestion script).
        - Spark job execution via `BashOperator` calling `spark-submit`.
        - Data validation task(s) post-processing.
    - **Task Dependencies:**
        - Proper dependency chains with error handling and retry logic.
        - SLA monitoring and alerting.
    - **Monitoring:**
        - Airflow UI-accessible dashboard for pipeline health (localhost:8082).
    - **Backfill Strategy:**
        - The DAG must support reprocessing historical date ranges on demand.

- **Provided Assets:**
    - `dags/dag_streamflow.py` - **Skeleton starter** for the orchestration DAG.

---

## 5. Technical Constraints

| Constraint | Requirement |
|------------|-------------|
| **Execution Environment** | (optional: Docker Compose, all services containerized ) |
| **Spark Execution** | `spark-submit` to `spark://spark-master:7077` |
| **Kafka Deployment** | Docker container (localhost:9094 external, kafka:9092 internal) |
| **Storage** | Docker volume mounts (`./data`, `./jobs`, `./dags`) |
| **Spark API** | DataFrame/Dataset APIs exclusively (No RDDs) |
| **Code Submission** | `spark-submit` compatible entry points |

---

## 6. Evaluation Criteria

| Category | Weight | Description |
|----------|--------|-------------|
| **Architecture Design** | 30% | Clarity of system design, justification of technical decisions. |
| **Code Quality** | 30% | Clean, modular, well-documented code. Proper error handling. |
| **Pipeline Functionality** | 30% | End-to-end execution: Kafka ingestion, Spark transformations, Airflow orchestration. |
| **Parameterization & Reusability** | 10% | Ability to run with different parameters and backfill historical data. |

---

## 7. Provided Starter Kit

The following assets are provided to eliminate DevOps friction:

| Asset | Purpose |
|-------|---------|
| `docker-compose.yml` | Complete Docker stack definition |
| `Dockerfile.airflow` | Custom Airflow image with JDK + Spark |
| `user_events_producer.py` | Mock data generator (upstream system) |
| `transaction_events_producer.py` | Mock data generator (upstream system) |
| `jobs/ingest_kafka_to_landing.py` | Skeleton - Kafka batch consumer |
| `jobs/spark_session_factory.py` | Skeleton - SparkSession factory |
| `jobs/etl_job.py` | Skeleton - PySpark transformation pipeline |
| `dags/dag_streamflow.py` | Skeleton - Airflow orchestration DAG |

> [!CAUTION]
> Skeleton files contain function signatures and docstrings but **no implementation**. Teams are to complete the TODO sections.

---

## 8. Deliverables

- Completed Kafka batch consumer implementation.
- PySpark DataFrame transformation pipeline (optional: tested on Docker Spark cluster).
- Completed Airflow DAG with full orchestration logic.
- Documentation: Architecture diagram, design decisions, setup instructions.
- Demo: Live walkthrough of end-to-end pipeline execution.

---

## 9. Quick Start (docker optional setup)

```bash
# Navigate to your project directory
# Build and start the entire stack
docker compose up -d --build

# Verify services are running
docker compose ps

# Access UIs:
# - Kafka UI: http://localhost:8080
# - Spark Master: http://localhost:8081
# - Airflow: http://localhost:8082 (admin/admin)

# Stop the stack
docker compose down 

# Nuclear Option
docker compose down --volumes

```
