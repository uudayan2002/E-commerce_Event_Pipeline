# Retail Transactional Intelligence Pipeline

## 1. Project Overview & Vision
This project is a containerized, **Big Data ETL pipeline** designed to solve the "Data Swamp" crisis for a simulated mid-sized UK-based retailer. It demonstrates how to orchestrate distributed data processing using **Apache Airflow**, **Apache Spark (PySpark)**, and **PostgreSQL**.

### The Problem: The "Data Swamp" Crisis
The company's sales data is currently untrustworthy. Exported directly from an aging legacy ordering system into massive, unwieldy Excel files (`online_retail_II.xlsx`), the data is plagued by anomalies that prevent accurate financial reporting and inventory management.

**Concrete Examples of the Messy Data Challenge:**
*   **Orphaned Transactions:** Over 20% of the dataset (approx. 200,000+ records) are missing `Customer ID`s, making it impossible to attribute revenue to specific users.
*   **Implicit Cancellations:** Returns and refunds are mixed into sales data. They are only identifiable by an 'C' prefix in the `Invoice` string (e.g., `C536379`) and often contain negative `Quantity` values that skew totals.
*   **Operational Noise:** The system uses the `StockCode` column for non-product costs. Codes like `POST` (postage), `M` (manual adjustments), and `D` (discounts) contaminate inventory analysis.
*   **Data Type Chaos:** As a raw Excel dump, the data lacks strict types, with dates and prices often imported as inconsistent strings.

---

## 2. The Solution: Automated Data Engineering
We are building an **Automated Data Factory** that replaces manual Excel wrangling with a scalable, auditable pipeline.

### **Apache Spark (PySpark) - The Compute Engine**
*   **Why:** Moving past Pandas. Processing 1.06 million rows on a single machine is a memory risk.
*   **Purpose:** Spark provides **distributed, in-memory processing**. By using the PySpark DataFrame API, we ensure that if the dataset grows to 100 million rows, the pipeline can scale horizontally across a cluster without code changes.

### **Apache Airflow - The Orchestrator**
*   **Why:** In production, ETL is a workflow with complex dependencies.
*   **Purpose:** Airflow manages the Directed Acyclic Graph (DAG). It doesn't process data; it acts as the "Manager" that submits heavy lifting to the Spark cluster via the `SparkSubmitOperator` and handles retries and logging.

### **PostgreSQL - The Analytical Warehouse**
*   **Why:** To enforce strict schema integrity for business analysts.
*   **Purpose:** We implemented a **Medallion-lite Architecture**:
    *   **Raw Layer (`raw_transactions`):** The immutable landing zone for auditing.
    *   **Clean Layer (`clean_transactions`):** The "Source of Truth" featuring strictly typed columns, primary keys, and a computed `total_amount` column.
    *   **Error Layer (`invalid_transactions`):** A quarantine zone for bad data, stored as JSON for reconciliation.

---

## 3. Data Architecture Layers

| Layer | Table Name | Business Purpose |
| :--- | :--- | :--- |
| **Staging** | `raw_transactions` | Landing zone for raw ingestion. Minimal constraints to prevent ingest failure. |
| **Core** | `clean_transactions` | Fully typed and validated core sales data. |
| **Audit** | `invalid_transactions` | Stores rejected records with specific `rejection_reason` codes. |
| **Gold** | `daily_sales_summary` | Fast-query table for daily revenue, order counts, and customer activity. |
| **Gold** | `product_metrics` | Analytical table tracking lifetime revenue and return rates per product. |

---

## 4. How the Pipeline Works

1.  **Ingestion:** Airflow detects the massive `online_retail_II.xlsx` file.
2.  **Spark Submission:** Airflow submits a PySpark job to the Dockerized Spark Cluster.
3.  **Sanitization:** Spark processes the data in parallel, flagging cancellations, stripping operational noise (`POST`, `M`), and enforcing data types.
4.  **Quarantine:** Spark filters out rows with fatal errors (e.g., negative prices) and diverts them to the `invalid_transactions` table.
5.  **Loading:** Spark uses JDBC to perform a high-speed bulk load into PostgreSQL.
6.  **Intelligence:** Airflow triggers SQL aggregations to update the Gold-tier analytical tables.

---

## 5. Getting Started (Current Status)

> **Note:** The project is currently undergoing an architectural pivot to migrate from Pandas to PySpark. 

### Roadmap
- [x] Detailed Problem Statement & Vision
- [x] Transactional Database Schema Design
- [ ] **Next:** Update Docker Infrastructure (Airflow + Spark Cluster)
- [ ] **Next:** Implement PySpark ETL logic
- [ ] **Next:** Real-time integration with Kafka
- [ ] **Next:** Cloud Deployment (AWS S3/Redshift)
