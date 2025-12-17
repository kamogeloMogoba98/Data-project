# Sales Pipeline

A data engineering pipeline implementing the **Medallion Architecture (Bronze → Silver → Gold)** using **Apache Airflow**, **Python**, **MinIO (S3-compatible storage)**, **DuckDB / MotherDuck**, and **Metabase**.

---

##  Quick Start




1. **Clone the repository**:

```bash
git clone https://github.com/kamogeloMogoba98/Data-project.git
cd Data-project
```

2. **Create the local environment file** from the example template:

```bash
cp .env.example .env
```

3. **Add your MotherDuck Token** to the `.env` file:

```bash
nano .env
```
- Paste your free MotherDuck token as `MOTHERDUCK_TOKEN=md_your_actual_token_here`

4. **Initialize and start all Docker containers** (Airflow, MinIO, Metabase):

```bash
docker compose up -d
```

5. **Verify that all containers are running successfully**:

```bash
docker compose ps
```

6. **Check Airflow scheduler logs** to ensure DAGs are loading:

```bash
docker compose logs airflow-standalone -f
```

7. **Enter the Airflow container** if you need to inspect the local DuckDB file:

```bash
docker exec -it airflow-unified bash
ls -l /opt/airflow/airflow_home_dev/my_local.db
```

8. **Trigger the pipeline DAG** in Airflow:

- DAG Name: `pipeline`
- Trigger via Airflow UI or CLI:

```bash
airflow dags trigger pipeline
```

---

## 🌐 Project UI Endpoints

- **Airflow UI:** [http://localhost:8089](http://localhost:8089)
- **MinIO Console:** [http://localhost:9001](http://localhost:9001)
- **Metabase:** [http://localhost:3002](http://localhost:3002)

---

## 📁 Project Directory Structure

```
Data-project/
├── airflow_home_dev/        # Airflow home and local DuckDB storage
│   ├── dags/                # Main Orchestration Folder
│   │   ├── DimGenerators/   # Python scripts for data generation
│   │   ├── DimJsonFiles/    # Local raw JSON source files
│   │   ├── helper/          # Connection & Utility modules
│   │   │   ├── duckdbconc.py        # MotherDuck/DuckDB connection helper
│   │   │   ├── latestFolder.py      # S3 partition management
│   │   │   └── MinoS3_connection.py # MinIO connection logic
│   │   ├── table_insert/    # Medallion Layer Logic
│   │   │   ├── BronzeTable.py       # Ingestion (Raw to Bronze)
│   │   │   ├── Silvertables.py      # Transformation (Bronze to Silver)
│   │   │   └── Goldtable.py         # Aggregation (Silver to Gold)
│   │   └── Pipeline.py      # Main Airflow DAG
│   └── my_local.db          # Local DuckDB instance (Git Ignored)
├── .env                     # Environment secrets (Git Ignored)
├── .gitignore               # Project exclude rules
├── docker-compose.yaml      # Container orchestration (Airflow, MinIO, Metabase)
├── Dockerfile               # Custom Airflow image
├── Metabase.Dockerfile      # Custom Metabase image with DuckDB driver
├── README.md                # Project documentation
└── requirements.txt         # Python dependencies
```

---

## 🔐 Environment Variables

```plaintext
MOTHERDUCK_TOKEN=md_your_actual_token_here

```

⚠️ **Do not commit your real `.env` file**. Add it to `.gitignore`.

---

## 🧬 Data Model (Bronze → Silver → Gold)

- **Bronze (Raw):** DuckDB schema `bronze`, JSON in MinIO, tables: `dim_customer`, `dim_product`, `dim_promotype`
- **Silver (Cleaned):** DuckDB schema `silver`, Parquet in MinIO, tables: `dim_customer`, `dim_product`, `dim_promo`
- **Gold (Aggregated):** DuckDB schema `gold`, Parquet in MinIO, table: `fact_sales`

---

## 🛠 Tools & Purpose

- **Apache Airflow:** DAG orchestration and task scheduling
- **Python 3:** ETL logic and data processing
- **MinIO:** S3-compatible object storage for JSON and Parquet files
- **DuckDB:** Local SQL database for schema layers
- **MotherDuck:** Cloud-hosted DuckDB for querying and analytics
- **Metabase:** Open-source BI tool to visualize data and create dashboards

---

## 🛡️ Final Checklist Before You Push

- [ ] `.env.example` exists and is committed
- [ ] `.env` is in `.gitignore`
- [ ] DAG loads successfully in Airflow UI
- [ ] Imports work without `sys.path` hacks
- [ ] Secrets are not hardcoded

---

## 👤 Author

**Kamogelo Mogoba**  
Data Engineer | linkedin kamogelo Mogoba 

---

## 📄 License

This project is for learning and portfolio purposes.

