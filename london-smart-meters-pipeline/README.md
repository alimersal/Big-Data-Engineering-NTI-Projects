<div align="center">
<p align="center">
  <!-- Row 1: Core Tech & Machine Learning -->
  <img src="https://img.shields.io/badge/Apache_Spark-3.5%2B-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/PySpark-MLlib-FF6B35?style=flat-square&logo=apachespark&logoColor=white" alt="PySpark"/>
  <img src="https://img.shields.io/badge/Hadoop-HDFS_3.3%2B-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black" alt="Hadoop"/>
  <img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/TensorFlow-2.x-FF6F00?style=flat-square&logo=tensorflow&logoColor=white" alt="TensorFlow"/>
  <img src="https://img.shields.io/badge/Keras-D00000?style=flat-square&logo=keras&logoColor=white" alt="Keras"/>
  <img src="https://img.shields.io/badge/scikit--learn-1.3%2B-F7931E?style=flat-square&logo=scikitlearn&logoColor=white" alt="scikit-learn"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white" alt="Pandas"/>
  <img src="https://img.shields.io/badge/NumPy-013243?style=flat-square&logo=numpy&logoColor=white" alt="NumPy"/>
  <img src="https://img.shields.io/badge/Matplotlib-11557C?style=flat-square&logo=python&logoColor=white" alt="Matplotlib"/>
  <img src="https://img.shields.io/badge/Seaborn-3776AB?style=flat-square&logo=python&logoColor=white" 
  alt="Seaborn"/>
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=26&pause=1000&color=E25A1C&center=true&vCenter=true&width=900&lines=%F0%9F%8F%A0+London+Smart+Meters+Pipeline;Big+Data+%7C+PySpark+%7C+ML+%7C+Deep+Learning;End-to-End+Data+Engineering+on+5%2C566+Households" alt="Typing SVG" />
  <img src="https://img.shields.io/badge/Dataset-Smart_Meters_in_London-20BEFF?style=flat-square&logo=kaggle&logoColor=white" alt="Dataset"/></a>
  <img src="https://img.shields.io/badge/Households-5%2C566-3498DB?style=flat-square" alt="Households"/>
  <img src="https://img.shields.io/badge/Daily_Records-3.3M%2B-E74C3C?style=flat-square" alt="Daily Records"/>
  <img src="https://img.shields.io/badge/HH_Sample-~7M_Rows-9B59B6?style=flat-square" alt="HH Sample"/>
  <img src="https://img.shields.io/badge/Features-40%2B-2ECC71?style=flat-square" alt="Features"/>
  <img src="https://img.shields.io/badge/ML_Tasks-8-F39C12?style=flat-square" alt="Tasks"/>
  <img src="https://img.shields.io/badge/NTI-Big_Data_Track-27AE60?style=flat-square" alt="NTI"/>
</p>
</div>

> End-to-end Big Data pipeline on **5,566 London households** smart meter readings (Nov 2011 – Feb 2014), built with Apache Spark and Python.
<hr>


## 📑 Table of Contents

| | |
|---|---|
| [📋 Overview](#-overview) | [🤖 ML Tasks](#-ml-tasks) |
| [🏗️ Pipeline Architecture](#️-pipeline-architecture) | [📊 Output Reports](#-output-reports) |
| [📓 Notebook Cells](#-notebook-cells) | [🎓 Skills Acquired](#-skills-acquired) |
| [📁 Project Structure](#-project-structure) | [🚀 Setup & Run](#-setup--run) |

---

## 📋 Overview

This project processes **half-hourly energy consumption** data from the UK Power Networks Low Carbon London trial. It covers the full data engineering lifecycle — ingestion, feature engineering, preprocessing, machine learning, and reporting — entirely within a distributed PySpark environment running locally.

<div align="center">

| 🏘️ Households | 📋 Daily Records | ⚡ Half-Hourly Rows | 🔧 Engineered Features | 🤖 ML Tasks |
|:---:|:---:|:---:|:---:|:---:|
| **~5,566** | **~3.3 M** | **~7 M** | **40+** | **8** |

</div>

---

## 🏗️ Pipeline Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                      DATA SOURCES (Kaggle)                                       ║
║  112 Block Daily CSVs (3.3M) │ Half-Hourly Sample (7M) │ Weather │ ACORN Map │ UK Bank Holidays  ║
╚═════════════════════════════════════════════════╦════════════════════════════════════════════════╝
                                                  │
                                         ┌────────▼────────┐
                                         │   Hadoop HDFS   │  ← Resilient Distributed Storage
                                         └────────┬────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │  Apache Spark   │  ← SparkSession (local[*], 8 GB RAM)
                                         │  PySpark 3.5+   │
                                         └────────┬────────┘
                                                  │
                         ┌────────────────────────┴────────────────────────┐
                         ▼                                                 ▼
          ┌─────────────────────────────┐                   ┌─────────────────────────────┐
          │     Data Preprocessing      │                   │     Feature Engineering     │
          │ • Median Imputation         │                   │ • Lags: 1d, 7d, 14d         │
          │ • Winsorization (1%–99%)    │                   │ • Rolling: 7d, 14d, 30d     │
          │ • Schema Cast & RAM Cache   │                   │ • Cyclical Time Encodings   │
          │ • Outlier Record Filtering  │                   │ • Weather Polynomial Joins  │
          └──────────────┬──────────────┘                   └──────────────┬──────────────┘
                         │                                                 │
                         └────────────────────────┬────────────────────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │  MLlib & Keras  │  ← 8 ML Pipelines
                                         └────────┬────────┘
                                                  │
                         ┌────────────────────────┼────────────────────────┐
                         ▼                        ▼                        ▼
             ┌─────────────────────┐        ┌─────────────────────┐        ┌─────────────────────┐
             │   HTML Dashboards   │        │   CSV Metric Logs   │        │  PNG Visualizations │
             └─────────────────────┘        └─────────────────────┘        └─────────────────────┘
```

---

## 📓 Notebook Cells

| # | Cell | Purpose |
|:---:|---|---|
| 1 | Environment Setup | Install deps, configure SparkSession, register `ensure_spark()` |
| 2 | Define Paths & Load Households | Set `DATA_PATH`, load `informations_households.csv` |
| 3 | Load ACORN Details | Socio-economic classification data, group averages |
| 4 | Load Weather Data | Daily & hourly weather, add season/weekend flags |
| 5 | Load Bank Holidays | UK bank holiday dates for `is_holiday` feature |
| 6 | Load Daily Dataset | 112 block CSVs → 3.3 M records, cast & cache |
| 7 | Load Half-Hourly Data | 5-block sample → 7 M rows, parse timestamps & slots |
| 8 | Feature Engineering (Daily) | Lag 1/7/14d, rolling 7/14/30d, CV, peak-to-mean, weather join |
| 9 | Advanced HH Features | Peak/off-peak/night ratios from half-hourly data |
| 10 | Data Preprocessing | Median imputation, winsorization (1–99%), row filtering |
| 11 | Task 1 — ACORN Classification | 3-class classification (Affluent / Adversity / Comfortable) |
| 12 | Task 2 — Tariff Classification | Binary (Std vs ToU), ranked by AUC |
| 13 | Task 3 — Energy Forecasting | Next-day energy_sum regression |
| 14 | Task 4 — Peak Demand Prediction | Daily energy_max regression |
| 15 | Task 5 — Segmentation | Load shape clustering + household behavioural clustering |
| 16 | Task 6 — Anomaly Detection | Z-Score & IQR dual-method detection |
| 17 | Task 7 — Weather Impact | Weather interaction features + regression |
| 18 | Task 8 — Neural Network | 4-branch multi-input Keras model |
| 19 | Results Summary | Consolidated dashboard for all 8 tasks |

---

## 🤖 ML Tasks

| Task | Type | Models | Metric |
|---|---|---|---|
| 1 — ACORN Classification | Multi-class Classification | LR, RF, DT, MLP, LinearSVM | F1 (weighted) |
| 2 — Tariff Classification | Binary Classification | LR, DT, RF | AUC |
| 3 — Energy Forecasting | Regression | LinearReg, RF | RMSE / R² |
| 4 — Peak Demand | Regression | RF, GBT, LinearReg | RMSE / R² |
| 5 — Segmentation | Clustering | KMeans (K=2–6) | Silhouette |
| 6 — Anomaly Detection | Unsupervised | Z-Score + IQR | Count / Overlap |
| 7 — Weather Impact | Feature Eng + Regression | LR, RF, GBT | R² / Feature Imp. |
| 8 — Neural Network | Deep Learning | 4-branch Keras Model | RMSE / MAE / R² |

---

## 📊 Output Reports

| Task | Report | CSV |
|:---|:---:|:---:|
| Task 1 — ACORN Classification | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%201/task01_acorn_20260827_044504.html) | [📥 CSV](output/Task%201/task01_acorn_20260827_044504.csv) |
| Task 2 — Tariff Classification | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%202/task02_tariff_20260827_044639.html) | [📥 CSV](output/Task%202/task02_tariff_20260827_044639.csv) |
| Task 3 — Energy Forecasting | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%203/task03_forecast_20260827_044857.html) | [📥 CSV](output/Task%203/task03_forecast_20260827_044857.csv) |
| Task 4 — Peak Demand | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%204/task04_peak_20260827_045011.html) | [📥 CSV](output/Task%204/task04_peak_20260827_045011.csv) |
| Task 5 — Segmentation | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%205/task05_seg_20260827_045209.html) | [📥 CSV](output/Task%205/task05_seg_20260827_045209.csv) |
| Task 6 — Anomaly Detection | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%206/task06_anomaly_20260827_050015.html) | [📥 CSV](output/Task%206/task06_anomaly_20260827_050015.csv) |
| Task 7 — Weather Impact | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%207/task07_weather_20260827_050125.html) | [📥 CSV](output/Task%207/task07_weather_20260827_050125.csv) |
| Task 8 — Neural Network | [📄 View Report](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/Task%208/task08_neural_network_20260827_050315.html) | [📥 CSV](output/Task%208/task08_neural_network_20260827_050315.csv) |
| Results Summary | [📄 View Dashboard](https://htmlpreview.github.io/?https://github.com/alimersal/Big-Data-Engineering-NTI-Projects/blob/master/london-smart-meters-pipeline/output/RESULTS_SUMMARY/results_summary_20260827_051008.html) | [📥 CSV](output/RESULTS_SUMMARY/results_summary_20260827_051008.csv) |

---

## 🎓 Skills Acquired

| Skill | Description |
|---|---|
| ![Spark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Distributed data processing with SparkSession, DataFrames, MLlib pipelines |
| ![Feature Eng](https://img.shields.io/badge/Feature%20Eng-4CAF50?style=flat-square&logo=python&logoColor=white) | Lag features, rolling windows, cyclical encoding, interaction terms |
| ![Classification](https://img.shields.io/badge/Classification-1565C0?style=flat-square&logo=scikitlearn&logoColor=white) | Logistic Regression, Random Forest, Decision Tree, MLP, Linear SVM |
| ![Regression](https://img.shields.io/badge/Regression-6A1B9A?style=flat-square&logo=scikitlearn&logoColor=white) | Linear Regression, Random Forest Regressor, GBT Regressor |
| ![Clustering](https://img.shields.io/badge/Clustering-EF6C00?style=flat-square&logo=python&logoColor=white) | KMeans, silhouette scoring, optimal K selection, PCA visualization |
| ![Anomaly](https://img.shields.io/badge/Anomaly%20Det-C62828?style=flat-square&logo=python&logoColor=white) | Z-Score + IQR dual-method unsupervised detection |
| ![Deep Learning](https://img.shields.io/badge/Deep%20Learning-FF6F00?style=flat-square&logo=tensorflow&logoColor=white) | Multi-input Keras model with 4 branches, BatchNorm, EarlyStopping |
| ![Preprocessing](https://img.shields.io/badge/Preprocessing-00695C?style=flat-square&logo=pandas&logoColor=white) | Median imputation, winsorization, missing value analysis |
| ![Big Data](https://img.shields.io/badge/Big%20Data-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Handling 7.5 GB half-hourly data, partitioning, caching, OOM management |
| ![Visualization](https://img.shields.io/badge/Visualization-11557C?style=flat-square&logo=plotly&logoColor=white) | Matplotlib dashboards, Seaborn heatmaps, HTML reports |

---

## 📁 Project Structure

```
london-smart-meters-pipeline/
│
├── 📓 london-smart-meters-pyspark-ml.ipynb   ← Main notebook (19 cells)
│
├── 📂 hadoop/
│   └── bin/winutils.exe                      ← Windows Hadoop binaries
│
└── 📂 output/
    ├── Task 1/           ← ACORN Classification
    ├── Task 2/           ← Tariff Classification
    ├── Task 3/           ← Energy Forecasting
    ├── Task 4/           ← Peak Demand
    ├── Task 5/           ← Segmentation
    ├── Task 6/           ← Anomaly Detection
    ├── Task 7/           ← Weather Impact
    ├── Task 8/           ← Neural Network
    └── RESULTS_SUMMARY/  ← Consolidated Dashboard
```

---

## 🚀 Setup & Run

```bash
# 1. Clone
git clone https://github.com/alimersal/london-smart-meters-pipeline.git
cd london-smart-meters-pipeline

# 2. Install dependencies (also handled automatically by Cell 1)
pip install pyspark==3.5.1 findspark pandas numpy matplotlib seaborn kagglehub scikit-learn tensorflow

# 3. Launch
jupyter notebook london-smart-meters-pyspark-ml.ipynb

# 4. Run all cells top to bottom — dataset downloads automatically via kagglehub
```

> **Requirements:** Python 3.11+ · Java 17 · 16 GB RAM recommended

---

## 👤 Author

<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=24&pause=1000&color=E25A1C&center=true&vCenter=true&width=550&lines=Ali+Mersal;Big+Data+Engineer+%7C+Software+Engineer;NTI+Big+Data+Engineering+Track+2026" alt="Author" />

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/alimersal)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0nMjU2JyBoZWlnaHQ9JzI1NicgeG1sbnM9J2h0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnJyBwcmVzZXJ2ZUFzcGVjdFJhdGlvPSd4TWlkWU1pZCcgdmlld0JveD0nMCAwIDI1NiAyNTYnPjxwYXRoIGQ9J00yMTguMTIzIDIxOC4xMjdoLTM3LjkzMXYtNTkuNDAzYzAtMTQuMTY1LS4yNTMtMzIuNC0xOS43MjgtMzIuNC0xOS43NTYgMC0yMi43NzkgMTUuNDM0LTIyLjc3OSAzMS4zNjl2NjAuNDNoLTM3LjkzVjk1Ljk2N2gzNi40MTN2MTYuNjk0aC41MWEzOS45MDcgMzkuOTA3IDAgMCAxIDM1LjkyOC0xOS43MzNjMzguNDQ1IDAgNDUuNTMzIDI1LjI4OCA0NS41MzMgNTguMTg2bC0uMDE2IDY3LjAxM1pNNTYuOTU1IDc5LjI3Yy0xMi4xNTcuMDAyLTIyLjAxNC05Ljg1Mi0yMi4wMTYtMjIuMDA5LS4wMDItMTIuMTU3IDkuODUxLTIyLjAxNCAyMi4wMDgtMjIuMDE2IDEyLjE1Ny0uMDAzIDIyLjAxNCA5Ljg1MSAyMi4wMTYgMjIuMDA4QTIyLjAxMyAyMi4wMTMgMCAwIDEgNTYuOTU1IDc5LjI3bTE4Ljk2NiAxMzguODU4SDM3Ljk1Vjk1Ljk2N2gzNy45N3YxMjIuMTZaTTIzNy4wMzMuMDE4SDE4Ljg5QzguNTgtLjA5OC4xMjUgOC4xNjEtLjAwMSAxOC40NzF2MjE5LjA1M2MuMTIyIDEwLjMxNSA4LjU3NiAxOC41ODIgMTguODkgMTguNDc0aDIxOC4xNDRjMTAuMzM2LjEyOCAxOC44MjMtOC4xMzkgMTguOTY2LTE4LjQ3NFYxOC40NTRjLS4xNDctMTAuMzMtOC42MzUtMTguNTg4LTE4Ljk2Ni0xOC40NTMnIGZpbGw9JyNmZmYnLz48L3N2Zz4K&logoColor=white)](https://www.linkedin.com/in/ali-mersal-7116641bb/)

<hr>

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&size=16&pause=3000&color=888888&center=true&vCenter=true&width=650&lines=%E2%AD%90+Star+this+repo+if+you+found+it+helpful!;Built+with+Apache+Spark+%7C+PySpark+%7C+Keras+%7C+Python" alt="Footer" />

</div>

