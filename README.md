🚀 Unified Data Engineering Pipeline: Airflow, MinIO, & Metabase
This project is a containerized end-to-end data pipeline built for seamless deployment. It uses Apache Airflow for orchestration, MinIO for object storage, and Metabase for data visualization. The environment is fully automated using Docker, ensuring that all Python dependencies (DuckDB, Pandas, Boto3) are pre-installed upon launch.

🛠️ Project Architecture
Orchestration: Apache Airflow 2.10.3 (SequentialExecutor).

Storage: MinIO (S3-compatible object storage).

Database: DuckDB for local analytical processing.

Visualization: Metabase for BI and dashboarding.

📂 Repository Structure
airflow_home_dev/dags: Contains the Python ETL logic.

airflow_home_dev/: Persistent volume for local database files and raw data.

Dockerfile: Custom build to automate library installations.

docker-compose.yaml: The master orchestration file for the entire stack.

🚀 Getting Started (Seamless Deployment)
Follow these steps to deploy the entire environment on any machine with Docker installed:

1. Clone the Repository

git clone https://github.com/kamogeloMogoba98/Data-project.git
cd Data-project



2. Initialize the EnvironmentBefore launching, you must create the metadata database file and set permissions to prevent SQLite locking errors:Bashtouch airflow.db
chmod -R 777 airflow_home_dev airflow.db

3. Launch with DockerUse the --build flag to ensure all Python libraries are correctly baked into the image:Bashdocker compose up -d --build
🔗 Service AccessOnce the containers are healthy, access the services via these URLs:ServiceURLCredentialsAirflowhttp://localhost:8089admin / adminMinIO Consolehttp://localhost:9001admin / passwordMetabasehttp://localhost:3002Setup on first visit🧹 CleanupTo stop the services and remove the containers while keeping your data safe:Bashdocker compose down
