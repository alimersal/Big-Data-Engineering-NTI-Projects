<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=32&pause=1000&color=38BDF8&center=true&vCenter=true&width=900&lines=%F0%9F%8E%93+NTI+Big+Data+Engineering+Track;End-to-End+Big+Data+Projects+Portfolio;Apache+Spark+%7C+Kafka+%7C+Hadoop+%7C+ML" alt="Typing SVG" />

<br/>

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.5-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Hadoop](https://img.shields.io/badge/Apache_Hadoop-3.2+-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-2.x-FF6F00?style=flat-square&logo=tensorflow&logoColor=white)](https://www.tensorflow.org/)
[![NTI](https://img.shields.io/badge/NTI-Big_Data_Track_2026-007ACC?style=flat-square&logo=databricks&logoColor=white)](https://nti.sci.eg/)

<br/>

<p align="center">
  <img src="https://user-images.githubusercontent.com/74038190/212284100-561aa473-3905-4a80-b561-0d28506553ee.gif" width="700">
</p>

</div>

---

## Abstract

Two end-to-end **Big Data Engineering** projects built as part of the **NTI Big Data Engineering Track (2026)**, covering the full modern data stack — real-time stream ingestion, distributed batch processing, machine learning at scale, and live visualization.

**Project 1** — A real-time pipeline ingesting live global aircraft telemetry every 15 seconds via Apache Flume → Kafka → Spark Structured Streaming → InfluxDB + Grafana, with raw events archived in Hadoop HDFS, fully containerized across 11 Docker services.

**Project 2** — A batch analytics and ML pipeline over 5,566 London households' smart meter readings (Nov 2011 – Feb 2014), covering ingestion, 40+ engineered features, preprocessing, and 8 ML tasks (classification, regression, clustering, anomaly detection, and deep learning) using PySpark MLlib + Keras.

---

## Projects

<br/>

### Project 1 — Real-Time Flight Tracker Pipeline

> **`flight_tracker-pipeline/`**

An enterprise-grade **real-time Big Data pipeline** that fetches live aircraft state vectors from the OpenSky Network every 15 seconds, routes them through a two-agent Apache Flume pipeline (HTTP Source → Kafka Sink & HDFS Sink), and processes them in Spark Structured Streaming to produce 7 InfluxDB measurements visualized on auto-provisioned Grafana dashboards. Raw events are simultaneously archived in a time-partitioned Hadoop HDFS data lake — all running via a single `docker compose up` across **11 containerized services**.

| Pillar | Technology | Role |
|:-------|:-----------|:-----|
| Live Ingestion | OpenSky API → Flume → Kafka | Global aircraft state vectors every 15 s |
| Stream Processing | Spark Structured Streaming | Real-time micro-batch analytics → 7 measurements |
| Data Lake | Hadoop HDFS | Time-partitioned raw JSON log archival |
| Visualization | InfluxDB + Grafana | Live interactive dashboards, auto-provisioned |
| Orchestration | Docker Compose | 11-service full-stack lifecycle management |

**Key Stats:** 11 Docker services · 7 InfluxDB measurements · 3 Kafka topics · 15-second ingestion cadence  
**[View Project →](flight_tracker-pipeline/README.md)**

---

### Project 2 — London Smart Meters — Big Data & ML Pipeline

> **`london-smart-meters-pipeline/`**

An end-to-end **batch Big Data and ML pipeline** on the UK Power Networks Low Carbon London dataset — **5,566 households**, **3.3M daily records**, **~7M half-hourly rows** (Nov 2011 – Feb 2014). Data is ingested from Kaggle into Hadoop HDFS, then preprocessed (median imputation, winsorization) and enriched with 40+ engineered features (lag, rolling windows, cyclical encodings, weather joins). The ML layer runs **8 PySpark MLlib + Keras pipelines** covering classification, regression, clustering, anomaly detection, and a 4-branch deep neural network — each producing an interactive HTML report and CSV metrics log.

| Task | Type | Approach |
|:-----|:-----|:---------|
| 1 — ACORN Classification | Multi-class | LR, RF, DT, MLP, LinearSVM (F1 weighted) |
| 2 — Tariff Classification | Binary | LR, DT, RF (AUC) |
| 3 — Energy Forecasting | Regression | Linear Reg, RF (RMSE / R²) |
| 4 — Peak Demand Prediction | Regression | RF, GBT, LinearReg (RMSE / R²) |
| 5 — Load Segmentation | Clustering | KMeans K=2–6 (Silhouette) |
| 6 — Anomaly Detection | Unsupervised | Z-Score + IQR dual-method |
| 7 — Weather Impact | Feature Eng + Reg | LR, RF, GBT (R² / Feature Importance) |
| 8 — Neural Network | Deep Learning | 4-branch Keras model (RMSE / MAE / R²) |

**Key Stats:** 5,566 households · 3.3M daily records · 7M half-hourly rows · 40+ features · 8 ML tasks  
**[View Project →](london-smart-meters-pipeline/README.md)**

---

## Combined Skills Demonstrated

| Domain | Capabilities |
|:-------|:-------------|
| **Stream Ingestion** | Apache Flume multi-agent pipelines, HTTP sources, Kafka & HDFS sinks |
| **Message Brokering** | Kafka cluster management, topic partitioning, consumer groups, ZooKeeper |
| **Stream Processing** | PySpark Structured Streaming, micro-batch triggers, Flux SQL analytics |
| **Batch Processing** | PySpark DataFrames, distributed joins, caching, OOM management on 7.5 GB |
| **Data Lake Design** | HDFS administration, WebHDFS REST API, time-partitioned log storage |
| **Feature Engineering** | Lag/rolling features, cyclical encodings, weather polynomial joins |
| **Machine Learning** | Classification, Regression, Clustering, Anomaly Detection (MLlib + scikit-learn) |
| **Deep Learning** | Multi-input Keras architecture, BatchNorm, EarlyStopping, TensorFlow 2.x |
| **Time-Series Storage** | InfluxDB Line Protocol, Flux queries, bucket management, retention policies |
| **Visualization** | Grafana auto-provisioning, InfluxDB dashboards, Matplotlib/Seaborn HTML reports |
| **Containerization** | Docker Compose orchestration, custom Dockerfiles, service health checks |
| **Pipeline Architecture** | Lambda architecture — real-time speed layer + batch cold-storage layer |

---

## 👤 Author

<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=24&pause=1000&color=E25A1C&center=true&vCenter=true&width=550&lines=Ali+Mersal;Big+Data+Engineer+%7C+Software+Engineer;NTI+Big+Data+Engineering+Track+2026" alt="Author" />

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/alimersal)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0nMjU2JyBoZWlnaHQ9JzI1NicgeG1sbnM9J2h0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnJyBwcmVzZXJ2ZUFzcGVjdFJhdGlvPSd4TWlkWU1pZCcgdmlld0JveD0nMCAwIDI1NiAyNTYnPjxwYXRoIGQ9J00yMTguMTIzIDIxOC4xMjdoLTM3LjkzMXYtNTkuNDAzYzAtMTQuMTY1LS4yNTMtMzIuNC0xOS43MjgtMzIuNC0xOS43NTYgMC0yMi43NzkgMTUuNDM0LTIyLjc3OSAzMS4zNjl2NjAuNDNoLTM3LjkzVjk1Ljk2N2gzNi40MTN2MTYuNjk0aC41MWEzOS45MDcgMzkuOTA3IDAgMCAxIDM1LjkyOC0xOS43MzNjMzguNDQ1IDAgNDUuNTMzIDI1LjI4OCA0NS41MzMgNTguMTg2bC0uMDE2IDY3LjAxM1pNNTYuOTU1IDc5LjI3Yy0xMi4xNTcuMDAyLTIyLjAxNC05Ljg1Mi0yMi4wMTYtMjIuMDA5LS4wMDItMTIuMTU3IDkuODUxLTIyLjAxNCAyMi4wMDgtMjIuMDE2IDEyLjE1Ny0uMDAzIDIyLjAxNCA5Ljg1MSAyMi4wMTYgMjIuMDA4QTIyLjAxMyAyMi4wMTMgMCAwIDEgNTYuOTU1IDc5LjI3bTE4Ljk2NiAxMzguODU4SDM3Ljk1Vjk1Ljk2N2gzNy45N3YxMjIuMTZaTTIzNy4wMzMuMDE4SDE4Ljg5QzguNTgtLjA5OC4xMjUgOC4xNjEtLjAwMSAxOC40NzF2MjE5LjA1M2MuMTIyIDEwLjMxNSA4LjU3NiAxOC41ODIgMTguODkgMTguNDc0aDIxOC4xNDRjMTAuMzM2LjEyOCAxOC44MjMtOC4xMzkgMTguOTY2LTE4LjQ3NFYxOC40NTRjLS4xNDctMTAuMzMtOC42MzUtMTguNTg4LTE4Ljk2Ni0xOC40NTMnIGZpbGw9JyNmZmYnLz48L3N2Zz4K&logoColor=white)](https://www.linkedin.com/in/ali-mersal-7116641bb/)
<hr>
</div>
