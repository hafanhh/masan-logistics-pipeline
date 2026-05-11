# Masan Logistics Data Pipeline
> Simulating Palantir Foundry Architecture — Processing 1M+ rows with PySpark using Medallion Architecture (Bronze → Silver → Gold)

---

## Overview

At **Masan Group**, logistics data comes from separate systems: ERP (orders), CRM (customers), and Logistics (coordinates). These sources have inconsistent schemas, uncertain data quality, and volumes reaching millions of records.

This project simulates the **Palantir Foundry** data pipeline process — from connecting raw data sources to creating analyst-ready KPIs — using the OLIST Brazil e-commerce dataset as a substitute.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   ERP (Orders)      CRM (Customers)     Logistics (Geolocation) │
│   99,441 rows       99,441 rows         1,000,163 rows          │
└────────────┬───────────────┬─────────────────┬──────────────────┘
             │               │                 │
             ▼               ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                         │
│  connector.py — Define Sources, automatic schema validation     │
│  schema_validator.py — Check join compatibility between sources │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw)                          │
│  spark_processor.py                                             │
│  • Join 3 sources with PySpark                                  │
│  • Dedup geolocation: 1,000,163 → 19,023 unique zip codes       │
│  • Output: Parquet + Snappy (~88% smaller than original CSV)   │
│  • 99,515 rows | 18 columns                                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleaned)                      │
│  silver_processor.py                                            │
│  • Fix 5 timestamp columns: string → datetime                   │
│  • Impute 278 orders missing coordinates (avg by state)        │
│  • Detect and fix 8 coordinates outside Brazil boundaries      │
│  • Calculate: actual_delivery_days, delay_days, delivery_speed  │
│  • Partition by year/month → faster queries                     │
│  • Data Quality: 4/4 checks PASS                                │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (Analyst-Ready)                  │
│  gold_processor.py                                              │
│  • KPI 1: Delivery performance by State (27 states)            │
│  • KPI 2: Order trend by month (2016–2018)                      │
│  • KPI 3: Late order analysis by severity                       │
│  • KPI 4: Executive Summary                                     │
│  • Output: CSV + Parquet ready for analysts                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Insights from Data

| Metric | Value | Meaning for Masan |
|--------|-------|-------------------|
| Total Orders | 99,515 | — |
| Success Rate | 97.0% | Logistics partner benchmark |
| Avg Delivery Days | 12.5 days | SLA baseline |
| Late Rate | 6.6% (6,539 orders) | Target for improvement |
| Fast Delivery ≤7 days | 30.9% | Potential to improve experience |
| Slowest State | RR — 29.3 days | Needs separate SLA |
| Fastest State | SP — 8.7 days | Internal benchmark |
| Peak Month | 2017-11 (Black Friday) | Late rate increased 4% → 12% |
| CRITICAL Orders (late >14 days) | 1,385 orders, max 188 days | Priority for complaint handling |

---

## Project Structure

```
masan_palantir/
│
├── data_source/                    # Raw CSV inputs (not committed to Git)
│   ├── orders.csv                  # ERP: 99,441 orders
│   ├── customers.csv               # CRM: 99,441 customers
│   └── geolocation.csv             # Logistics: 1,000,163 coordinates
│
├── config.py                       # Configuration settings
├── main.py                         # Main pipeline orchestrator
├── connector.py                    # Data Ingestion — connect and validate sources
├── schema_validator.py             # Schema Enforcement — check join compatibility
├── spark_processor.py              # Bronze Layer — join 3 sources with PySpark
├── silver_processor.py             # Silver Layer — clean, normalize, derived columns
├── gold_processor.py               # Gold Layer — KPI, aggregation, executive summary
├── dashboard.py                    # Streamlit dashboard for KPI visualization
├── visualize.py                    # Static chart generation
├── debug_quality.py                # Data quality debugging utilities
│
├── bronze_layer/                   # Bronze Layer output (Parquet/Snappy)
│   └── orders_enriched/
│       └── _SUCCESS
│
├── silver_layer/                   # Silver Layer output (Parquet, partitioned)
│   └── orders_cleaned/
│       ├── order_year=2016/
│       ├── order_year=2017/
│       └── order_year=2018/
│
├── gold_layer/                     # Gold Layer output (CSV + Parquet)
│   ├── executive_summary.csv
│   ├── kpi_delivery_by_state.csv
│   ├── kpi_monthly_trend.csv
│   └── kpi_late_orders.csv
│
├── charts/                         # Generated visualization charts
│   ├── chart_delivery_speed.png
│   ├── chart_late_rate_by_state.png
│   └── chart_monthly_trend.png
│
├── sample_output/                  # Sample output files for reference
│   ├── executive_summary_sample.csv
│   └── kpi_delivery_by_state_sample.csv
│
├── logs/                           # Pipeline execution logs
├── notebooks.ipynb                 # Jupyter notebook for exploration
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Docker container definition
├── docker-compose.yml              # Docker Compose configuration
└── README.md                       # This file
```

---

## Tech Stack

| Layer | Tool | Reason for Choice |
|-------|------|-------------------|
| Ingestion | Python + Pandas | Source connection, lightweight schema validation |
| Processing | PySpark 4.1 | Process 1M+ rows, parallel execution |
| Storage | Parquet + Snappy | Columnar format, good compression, fast queries |
| Partitioning | year/month | Optimize time-based queries |
| Visualization | Streamlit + Matplotlib | Interactive dashboard + static charts |
| Containerization | Docker + Docker Compose | Reproducible environment |
| Runtime | Java 17 + Python 3.10 | Compatible with PySpark 4.x |

---

## Palantir Foundry Mapping

Each component in this project corresponds to a concept in Palantir Foundry:

| This Project | Palantir Foundry | Description |
|-------------|-----------------|-------------|
| `connector.py` | Data Connection | Define sources, enforce schema upon connection |
| `schema_validator.py` | Schema Enforcement | Check join key compatibility before processing |
| `main.py` | Pipeline Orchestrator | Run entire pipeline with single command |
| Bronze Layer | Raw Dataset | Raw data, no transformation, maintain lineage |
| Silver Layer | Foundry Transform | Clean, normalize, calculate derived columns |
| Gold Layer | Ontology / View | Analyst-ready KPIs and dashboard |
| `dashboard.py` | Contour (Dashboard) | Interactive KPI visualization |
| Partition by year/month | Dataset Branches | Optimize access patterns by time |
| `debug_quality.py` | Data Quality Tools | Debug and validate data quality issues |

---

## Running the Pipeline

### Requirements

```bash
# Python 3.10+
pip install -r requirements.txt

# Java 17 (required for PySpark 4.x)
sudo apt install openjdk-17-jdk -y
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### Prepare Data

Download the dataset and place in `data_source/` directory with these names:

```
→ data_source/orders.csv
→ data_source/customers.csv
→ data_source/geolocation.csv
```

### Run Options

#### Option 1: Run Full Pipeline (Recommended)

```bash
# Run entire pipeline with single command
python main.py

# Run specific step only
python main.py --step bronze    # Bronze layer only
python main.py --step silver    # Silver layer only
python main.py --step gold      # Gold layer only
```

#### Option 2: Run Individual Components

```bash
# Step 1 — Check connections and schema
python connector.py

# Step 2 — Validate join compatibility between 3 sources
python schema_validator.py

# Step 3 — Create Bronze Layer (join + Parquet)
python spark_processor.py

# Step 4 — Create Silver Layer (clean + normalize)
python silver_processor.py

# Step 5 — Create Gold Layer (KPI + executive summary)
python gold_processor.py
```

#### Option 3: Run with Docker

```bash
# Build and run with Docker Compose
docker-compose up --build

# Or run individual container
docker build -t masan-pipeline .
docker run -v $(pwd)/data_source:/app/data_source -v $(pwd)/bronze_layer:/app/bronze_layer -v $(pwd)/silver_layer:/app/silver_layer -v $(pwd)/gold_layer:/app/gold_layer masan-pipeline
```

### Expected Results

```
connector.py      → 3 sources connected successfully, schema validated
schema_validator  → match rate 100% (orders↔customers), 99% (customers↔geo)
spark_processor   → bronze_layer/orders_enriched/ (Parquet)
silver_processor  → silver_layer/orders_cleaned/ (partitioned Parquet, 4/4 DQ checks PASS)
gold_processor    → gold_layer/*.csv (analyst-ready KPIs)
```

---

## Dashboard & Visualization

### Interactive Dashboard

```bash
# Run Streamlit dashboard
streamlit run dashboard.py

# Access at: http://localhost:8501
```

**Features:**
- Executive Summary KPIs
- Monthly order trends with late rate
- Delivery speed distribution
- State performance comparison
- Late order severity analysis
- Interactive filters by state

### Static Charts

```bash
# Generate static visualization charts
python visualize.py
```

**Outputs:**
- `charts/chart_monthly_trend.png` - Order trends over time
- `charts/chart_delivery_speed.png` - Delivery speed distribution
- `charts/chart_late_rate_by_state.png` - Late rates by state

---

## Charts

### Monthly Order Trend
![Monthly Trend](charts/chart_monthly_trend.png)

### Delivery Speed Distribution
![Delivery Speed](charts/chart_delivery_speed.png)

### Late Rate by State
![Late Rate by State](charts/chart_late_rate_by_state.png)

---

## Key Technical Highlights

**Schema-first approach** — Don't use `inferSchema=True`. All schemas are explicitly declared in `SPARK_SCHEMAS` and `EXPECTED_SCHEMAS`. This is a mandatory principle in Palantir Foundry to ensure data lineage.

**No data deletion in Bronze** — Bronze keeps all 99,515 rows, including null rows. Null handling only occurs in Silver, and all decisions are logged with clear reasons (`NOT_YET_DELIVERED`, `DATA_QUALITY_ISSUE`).

**Controlled imputation** — Missing coordinates are filled with averages from the same state, not the entire dataset average. The `geo_imputed` column marks imputed rows for analyst awareness.

**Data Quality gate** — Silver won't save if DQ checks FAIL. This is an important pattern in production pipelines to prevent bad data from spreading to Gold.

**Partition strategy** — Silver partitions by `order_year/order_month`. Query `WHERE order_year=2018 AND order_month=11` only reads 1 partition instead of the entire dataset.

---

## Dataset

The actual dataset includes 100k orders from 2016–2018, containing order information, customers, products, sellers, logistics, and reviews.

---

*Built as a Data Engineering portfolio project — simulating Palantir Foundry workflows with open-source tools.*