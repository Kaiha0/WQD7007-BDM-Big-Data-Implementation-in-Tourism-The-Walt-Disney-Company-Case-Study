# Big Data Implementation in Tourism: A Case Study of The Walt Disney Company

This project evaluates the performance of the **Hadoop ecosystem** by comparing **Apache Hive** and **Apache Spark** in analyzing Disneyland attraction waiting times.

## Table of Content
1. [Project Overview](#project-overview)
2. [Problem Statement](#problem-statement)
3. [System Architecture](#system-architecture)
4. [Dataset Description](#dataset-description)
5. [Implementation Pipelines](#implementation-pipelines)
    * [Pipeline A: Apache Hive (Batch)](#pipeline-a-apache-hive-batch)
    * [Pipeline B: Apache Spark (In-Memory)](#pipeline-b-apache-spark-in-memory)
6. [Advanced Predictive Analytics](#advanced-predictive-analytics)
7. [Performance Evaluation](#performance-evaluation)
8. [Conclusion](#conclusion)
9. [Disclaimer](#disclaimer)

---

## Project Overview
Disneyland requires high operational efficiency to manage guest flows. This project compares disk-based batch processing (Hive) against in-memory distributed computing (Spark) using a dataset of over **3.5 million records**.

**Key Objectives:**
* Estimate attraction waiting times using historical theme park data.
* Benchmark Spark (PySpark) vs. Hive (SQL) regarding performance and resource utilization.
* Transition from descriptive statistics to predictive wait-time estimation using MLlib.

---

## Problem Statement
Traditional RDBMS architectures often lack the horizontal scalability needed for multi-dimensional tourism datasets, leading to significant latency in guest flow analysis. This study addresses computational latency and the architectural trade-offs between batch and in-memory processing.

---

## System Architecture
The project utilizes a **dual-pipeline architecture** deployed on **Google Cloud Dataproc**, decoupling storage from computation.


* **Storage:** Google Cloud Storage (GCS) as a centralized "Bronze" source of truth.
* **Infrastructure:** 1 Master and 2 Worker nodes (n1-standard-4) in the `us-central1` region.

---

## Dataset Description
* **Source:** Historical Disneyland wait-time data.
* **Records:** 3,509,324.
* **Size:** ~375 MB.
* **Attributes:** Attraction name, wait time, timestamp, day type, and season.
Due to the file size (approx. 375 MB), the dataset is hosted externally.
* **Kaggle Source:** [Hackathon - Waiting Times Dataset](https://www.kaggle.com/datasets/ayushtankha/hackathon?select=waiting_times.csv)

---

## Implementation Pipelines

### Pipeline A: Apache Hive (Batch)
* **Architecture:** Schema-on-Read using external tables referencing GCS.
* **Process:** Data cleaning and aggregations implemented via HiveQL (HQL).

### Pipeline B: Apache Spark (In-Memory)
* **Architecture:** Direct loading into Spark DataFrames via native GCS connectors.
* **Process:** High-performance transformations and advanced analytics using PySpark.

---

## Advanced Predictive Analytics
Spark MLlib was used to develop predictive models, a capability not natively supported by Hive.

* **Best Model:** **Gradient Boosted Trees (GBT)**.
* **Performance:** $R^2$ of 0.8128 and MAE of 6.47 minutes.
* **Key Feature:** `WEEK_AVG_WAIT` accounted for 59.5% of predictive importance.

---

## Performance Evaluation
| Metrics | Spark (PySpark) | Hive (SQL) | Winner |
| :--- | :--- | :--- | :--- |
| **Total Execution Time** | **2.0 min** | 2.5 min | **Spark** |
| **Data Loading Time** | **0.40 min** | 0.51 min | **Spark** |
| **CPU Utilization** | **4.96%** | 6.49% | **Spark** |

**Summary:** Spark completed the workflow **20% faster** than Hive due to in-memory processing.

---

## Conclusion
While both engines are reliable, **Spark** is the superior platform for real-time tourism management due to its speed and native machine learning support. Hive remains effective for simpler, SQL-centric batch reporting.

---

## Disclaimer
This project was developed for **educational purposes**. While commercial usage is welcomed, the author is **not liable** for any losses or cloud service charges incurred. Users are responsible for monitoring their own Google Cloud billing.
