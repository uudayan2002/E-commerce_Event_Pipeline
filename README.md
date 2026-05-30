# Retail Transactional Intelligence & RAG Platform

## 1. Project Overview & Vision
This project is a containerized, **Big Data ETL & Knowledge Platform** designed to solve the "Data Swamp" crisis for a simulated mid-sized UK-based retailer. It showcases a modern **Medallion Architecture** that orchestrates distributed data processing using **Apache Airflow**, **Apache Spark (PySpark)**, and **PostgreSQL**.

### The Problem: The "Data Swamp" Crisis
The company's sales data was historically untrustworthy. Exported directly from an aging legacy ordering system into massive, unwieldy Excel files (`online_retail_II.xlsx`), the data is plagued by anomalies that prevent accurate financial reporting and inventory management:
*   **Orphaned Transactions:** Over 20% of the dataset (approx. 200,000+ records) are missing `Customer ID`s, making it impossible to attribute revenue to specific users.
*   **Implicit Cancellations:** Returns and refunds are mixed into sales data. They are only identifiable by a 'C' prefix in the `Invoice` string (e.g., `C536379`) and often contain negative `Quantity` values that skew totals.
*   **Operational Noise:** The system uses the `StockCode` column for non-product costs. Codes like `POST` (postage), `M` (manual adjustments), and `D` (discounts) contaminate inventory analysis.
*   **Data Type Chaos:** As a raw Excel dump, the data lacks strict types, with dates and prices often imported as inconsistent strings.

---

## 2. The Solution: Automated Data Engineering

We have built an **Automated Data Factory** that replaces manual Excel wrangling with a scalable, auditable, and strictly typed pipeline.

```
┌─────────────────┐       ┌─────────────────┐       ┌──────────────────┐
│   Raw Dataset   ├──────►│   Apache Spark  ├──────►│    PostgreSQL    │
│  (Excel 45MB)   │       │  (PySpark ETL)  │       │ (Medallion DB)   │
└─────────────────┘       └────────┬────────┘       └────────┬─────────┘
                                   │                         │
                                   ▼                         ▼
                          ┌─────────────────┐       ┌──────────────────┐
                          │ Apache Airflow  │       │ Analytics Layers │
                          │  (Orchestrator) │       │   (Gold SQLs)    │
                          └─────────────────┘       └──────────────────┘
```

### **Apache Spark (PySpark) - The Compute Engine**
*   **Why:** Moving past Pandas. Processing 1.06 million rows on a single machine is a memory risk.
*   **Purpose:** Spark provides **distributed, in-memory processing**. By using the PySpark DataFrame API, we ensure that if the dataset grows to 100 million rows, the pipeline can scale horizontally across a cluster without code changes. We run in an optimized `local[*]` container configuration to maximize resource efficiency.

### **Apache Airflow - The Orchestrator**
*   **Why:** In production, ETL is a workflow with complex dependencies.
*   **Purpose:** Airflow manages the Directed Acyclic Graph (DAG). It doesn't process data; it acts as the "Manager" that submits heavy lifting to the Spark cluster via the `SparkSubmitOperator` and triggers downstream database analytics once data loading is complete.

### **PostgreSQL - The Analytical Warehouse**
*   **Why:** To enforce strict schema integrity for business analysts.
*   **Purpose:** We implemented a **Medallion Architecture**:
    *   **Core Layer (`clean_transactions`):** The "Source of Truth" featuring strictly typed columns, primary keys, and a computed `total_amount` column.
    *   **Error Layer (`invalid_transactions`):** A quarantine zone for bad data, stored as JSONB for auditing and reconciliation.
    *   **Gold Layer (`daily_sales_summary` & `product_metrics`):** Fast-query analytics tables tracking daily financials and product-specific return rates.

---

## 3. Data Architecture Layers

| Layer | Table Name | Business Purpose |
| :--- | :--- | :--- |
| **Staging** | `raw_transactions` | Landing zone for raw ingestion. Minimal constraints to prevent ingest failure. |
| **Core** | `clean_transactions` | Fully typed and validated core sales data (11 strict columns, indexing on timestamps). |
| **Audit** | `invalid_transactions` | Stores rejected records with specific `rejection_reason` codes under a JSONB structure. |
| **Gold** | `daily_sales_summary` | Fast-query table tracking daily revenue, order counts, unique customers, and cancellations. |
| **Gold** | `product_metrics` | Analytical table tracking lifetime quantity sold, total revenue, and cancellation rates per product. |

---

## 4. Current Status & Achievements (100% Succeeded)

The core big data ETL pipeline is **fully functional and verified**. Our recent milestones include:
1.  **PySpark Ingestion Engine:** Successfully parses the 45MB raw Excel dataset, performs parallel data cleaning, isolates operational codes, drops raw intermediate timestamps, and uses JDBC to perform high-speed bulk loads.
2.  **Casting & Type Corrections:** Resolves a major float parsing bug in Excel (where customer IDs like `17850` were read as `17850.0`) by performing an internal `Double -> Long -> String` cast to maintain clean data types.
3.  **Gold Upsert Logic:** Built advanced SQL upsert scripts (`ON CONFLICT DO UPDATE`) in the Airflow Postgres operators to dynamically recalculate daily summaries and product statistics without duplicate key errors.
4.  **Security & Secrets Management:** Established `.env` and `.env.example` configurations to externalize environment variables, and configured a comprehensive `.gitignore` to prevent secret leaks.

---

## 5. Next Evolutionary Step: The RAG Knowledge Lakehouse

To maximize the value of this high-quality transactional data, we are evolving this pipeline into a **Retrieval-Augmented Generation (RAG) Platform**. This will enable business leaders to ask natural language questions (e.g., *"Which customers are showing a strong tendency to return items?"*) and receive accurate, context-rich answers.

### Why pgvector?
Instead of adding heavy external vector database containers (like ChromaDB or Milvus), we are integrating **`pgvector`** directly into PostgreSQL. This keeps our infrastructure compact, maintains strict ACID compliance, and allows unified queries merging transactional SQL joins with semantic cosine similarity searches.

### The Hybrid Retrieval Approach
LLMs are notoriously weak at mathematical operations over large datasets. Asking an LLM to sum 1 million rows will result in massive hallucinations. Our RAG engine employs a **Hybrid Retrieval Router**:
1.  **Qualitative & Behavioral Queries** (*"Summarize customer behavior..."*): Routed to `pgvector` semantic similarity search over pre-embedded entity profiles.
2.  **Quantitative & Mathematical Queries** (*"Who spent the most money today?"*): Routed to structured **Text-to-SQL** execution over our Core database layers.

```
                    ┌───────────────────────────────┐
                    │   User Query (Natural Lang)   │
                    └───────────────┬───────────────┘
                                    │
                            [RAG Query Router]
                                    │
                   ┌────────────────┴────────────────┐
                   ▼                                 ▼
       [Semantic Search (pgvector)]       [Structured Query (Text-to-SQL)]
    "Summarize customer behavior..."     "Who generated the highest revenue?"
                   │                                 │
                   ▼                                 ▼
       ┌───────────────────────┐         ┌───────────────────────┐
       │  Profile Embeddings   │         │    SQL Query Exec     │
       │  from pgvector Store  │         │    over Postgres DB   │
       └───────────┬───────────┘         └───────────┬───────────┘
                   │                                 │
                   └────────────────┬────────────────┘
                                    ▼
                        [Prompt Builder Context]
                                    │
                                    ▼
                          [LLM Generation (RAG)]
```

---

## 6. RAG Evolution Roadmap

### Phase 1: Infrastructure & pgvector Setup (Foundation)
*   Integrate `pgvector` and `sentence-transformers` libraries.
*   Update `docker-compose.yml` to compile or pull vector-enabled PostgreSQL.
*   Create new schema layers for vector profiles (`customer_profiles`, `product_profiles`, and `daily_performance_narratives`) equipped with high-performance **HNSW indexes**.

### Phase 2: Profiles & Embedding Pipeline (Data Engineering)
*   Develop `scripts/generate_embeddings.py` using PySpark or standard SQLAlchemy to build natural language summaries of customers and products.
*   Vectorize text profiles locally inside Docker using the `sentence-transformers` library (`all-MiniLM-L6-v2` model: 384 dimensions, offline, 100% free).
*   Integrate embedding generation as a new downstream task in the Airflow DAG running after Gold SQL updates.

### Phase 3: Retrieval & Tool Routing Engine (Software Architecture)
*   Create a python query controller (`scripts/rag_engine.py`) hosting the query routing engine.
*   Write a router that decides whether to fetch semantic embeddings, execute standard SQL queries (Text-to-SQL), or combine both to construct the prompt context.

### Phase 4: Premium UI & Evaluation Dashboard (Wow Factor)
*   Build a beautiful, responsive chat interface to interact with the **RAG Analytics Assistant**.
*   Log queries, retrieval scores, latency, and context coherence into an auditing table in PostgreSQL to demonstrate production-grade reliability.

---

## 7. Getting Started

### Prerequisites
*   Docker & Docker Compose installed on your host system.
*   At least 4GB of RAM allocated to Docker to support parallel Spark execution.

### Setup Instructions
1.  **Configure Environment Secrets:**
    Copy the template file to `.env` and fill in your desired passwords:
    ```bash
    cp .env.example .env
    ```

2.  **Start the Services:**
    Launch the containerized environment in the background:
    ```bash
    docker compose up -d
    ```

3.  **Trigger the ETL Pipeline:**
    Once all containers are running and healthy, trigger the ingestion run:
    ```bash
    docker exec airflow_scheduler airflow dags trigger retail_transactional_pipeline
    ```

4.  **Monitor the Logs:**
    Track the execution logs of the active PySpark run:
    ```bash
    docker exec -it airflow_scheduler tail -f /opt/airflow/logs/dag_id=retail_transactional_pipeline/run_id=<RUN_ID>/task_id=run_pyspark_retail_etl/attempt=1.log
    ```
