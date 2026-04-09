# 📦 Multimodal Video Data Pipeline (1-Month Infra Project)

## 🚀 Overview

This project simulates a **mini-scale multimodal data processing platform** for AI training.

It processes raw video data into structured datasets through:

- Frame extraction (OpenCV)
- Metadata generation
- Distributed processing (PySpark)
- Data quality validation
- Pipeline orchestration (Airflow)

---

## 🧠 Motivation

Transition from **Analytics Engineering (dbt/Snowflake)** to:

> **Data Infrastructure for AI / Multimodal Systems**

This project focuses on:

- Unstructured data (video/images)
- Distributed data pipelines
- Data production systems (not just consumption)

---

## 🏗 Architecture
Raw Video
↓
Ingestion
↓
Frame Extraction (OpenCV)
↓
Metadata Generation
↓
Storage (Parquet on S3/Local)
↓
Processing (PySpark)
↓
Data Quality Checks
↓
Train/Test Dataset Output

---

## 🧰 Tech Stack

| Layer | Tool |
|------|------|
| Language | Python |
| Processing | PySpark |
| Video | OpenCV |
| Storage | Parquet + S3/MinIO |
| Orchestration | Airflow |
| Analysis | DuckDB / Notebook |
