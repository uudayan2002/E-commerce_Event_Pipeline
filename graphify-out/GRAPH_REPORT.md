# Graph Report - .  (2026-05-30)

## Corpus Check
- Corpus is ~1,818 words - fits in a single context window. You may not need a graph.

## Summary
- 27 nodes · 26 edges · 6 communities detected
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 4 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_ETL Core (SparkPython)|ETL Core (Spark/Python)]]
- [[_COMMUNITY_Project Infrastructure|Project Infrastructure]]
- [[_COMMUNITY_Spark & Medallion Architecture|Spark & Medallion Architecture]]
- [[_COMMUNITY_Airflow Orchestration|Airflow Orchestration]]
- [[_COMMUNITY_Spark Configuration|Spark Configuration]]
- [[_COMMUNITY_Data Persistence (SQLAlchemy)|Data Persistence (SQLAlchemy)]]

## God Nodes (most connected - your core abstractions)
1. `E-commerce Event Pipeline` - 6 edges
2. `Retail Transactional Intelligence Pipeline` - 5 edges
3. `main()` - 4 edges
4. `extract_data()` - 3 edges
5. `transform_data()` - 3 edges
6. `load_data()` - 3 edges
7. `Apache Airflow` - 3 edges
8. `create_spark_session()` - 2 edges
9. `generate_events.py` - 2 edges
10. `Apache Spark (PySpark)` - 2 edges

## Surprising Connections (you probably didn't know these)
- `Retail Transactional Intelligence Pipeline` --semantically_similar_to--> `E-commerce Event Pipeline`  [INFERRED] [semantically similar]
  README.md → handoff.md
- `pandas` --conceptually_related_to--> `E-commerce Event Pipeline`  [INFERRED]
  requirements.txt → handoff.md
- `apache-airflow` --implements--> `Apache Airflow`  [INFERRED]
  requirements.txt → handoff.md
- `pyspark` --implements--> `Apache Spark (PySpark)`  [INFERRED]
  requirements.txt → README.md

## Communities

### Community 0 - "ETL Core (Spark/Python)"
Cohesion: 0.36
Nodes (7): extract_data(), load_data(), main(), Reads the Excel file into a Spark DataFrame.     Note: For Excel, we use Pandas, Applies business logic and sanitation:     1. Rename columns to match Postgres, Writes Spark DataFrame to PostgreSQL using JDBC., transform_data()

### Community 1 - "Project Infrastructure"
Cohesion: 0.33
Nodes (6): Docker Compose, E-commerce Event Pipeline, generate_events.py, PostgreSQL, Synthetic Data Generator, pandas

### Community 2 - "Spark & Medallion Architecture"
Cohesion: 0.33
Nodes (6): Apache Spark (PySpark), The Data Swamp Crisis, Medallion-lite Architecture, online_retail_II.xlsx, Retail Transactional Intelligence Pipeline, pyspark

### Community 3 - "Airflow Orchestration"
Cohesion: 0.67
Nodes (3): Apache Airflow, ecommerce_pipeline_dag.py, apache-airflow

### Community 4 - "Spark Configuration"
Cohesion: 1.0
Nodes (2): create_spark_session(), Initializes a Spark Session with JDBC driver support.

### Community 6 - "Data Persistence (SQLAlchemy)"
Cohesion: 1.0
Nodes (1): sqlalchemy

## Knowledge Gaps
- **15 isolated node(s):** `Initializes a Spark Session with JDBC driver support.`, `Reads the Excel file into a Spark DataFrame.     Note: For Excel, we use Pandas`, `Applies business logic and sanitation:     1. Rename columns to match Postgres`, `Writes Spark DataFrame to PostgreSQL using JDBC.`, `Synthetic Data Generator` (+10 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Spark Configuration`** (2 nodes): `create_spark_session()`, `Initializes a Spark Session with JDBC driver support.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Data Persistence (SQLAlchemy)`** (1 nodes): `sqlalchemy`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `E-commerce Event Pipeline` connect `Project Infrastructure` to `Spark & Medallion Architecture`, `Airflow Orchestration`?**
  _High betweenness centrality (0.222) - this node is a cross-community bridge._
- **Why does `Retail Transactional Intelligence Pipeline` connect `Spark & Medallion Architecture` to `Project Infrastructure`?**
  _High betweenness centrality (0.166) - this node is a cross-community bridge._
- **Why does `Apache Airflow` connect `Airflow Orchestration` to `Project Infrastructure`?**
  _High betweenness centrality (0.077) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `E-commerce Event Pipeline` (e.g. with `Retail Transactional Intelligence Pipeline` and `pandas`) actually correct?**
  _`E-commerce Event Pipeline` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Initializes a Spark Session with JDBC driver support.`, `Reads the Excel file into a Spark DataFrame.     Note: For Excel, we use Pandas`, `Applies business logic and sanitation:     1. Rename columns to match Postgres` to the rest of the system?**
  _15 weakly-connected nodes found - possible documentation gaps or missing edges._