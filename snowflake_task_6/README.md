# LMS Task 6 - Snowflake & Airflow ETL

## Technology Stack
* **Snowflake Data Cloud** (Data Warehouse, SQL Scripting, Streams, Tasks)
* **Apache Airflow** (Orchestration in Docker)
* **Docker & Docker Compose**
* **Power BI** (Business Intelligence & Visualization)

## Project Structure
* `docker-compose.yml` — Docker services configuration for Airflow.
* `dags/` — Python code for Airflow DAGs definition.
* `snowflake_airline_db_scripts/` — SQL scripts for database initialization, stored procedures, and security setup.
* `dataset/` — Directory containing the source `airline_dataset.csv`.
* `power_bi_exported_graphs/` — Exported analytics reports (.pdf/.pbix).

## Setup and Running

1.  **Clone the repository:**

2.  **Configure Environment:**
    Create a `.env` file in the project root with the following variables:
    ```ini
    AIRFLOW_UID=50000
    SNOWFLAKE_ACCOUNT=your_account_id
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_WAREHOUSE=COMPUTE_WH
    SNOWFLAKE_DATABASE=AIRLINE_DB
    SNOWFLAKE_SCHEMA=RAW_DATA
    ```

3.  **Start Containers:**
    Execute the command in the project root:
    ```bash
    docker-compose up -d --build
    ```

4.  **Initialize Snowflake Infrastructure:**
    Execute SQL scripts from `snowflake_airline_db_scripts/` in your Snowflake account in the following order:
    1.  Setup Infrastructure (DB, Schemas, Stage).
    2.  Create Tables & Procedures.
    3.  Setup Security & RLS.

5.  **Upload Data:**
    Upload `Airline Dataset.csv` from the `dataset/` folder to the Snowflake Internal Stage (`@RAW_DATA.AIRLINE_STAGE`).

## How to Configure Airflow Connection

To enable Airflow to communicate with Snowflake, you need to configure the connection inside the Airflow UI.

### Step 1: Access Airflow UI
The Airflow webserver will be accessible at:
`http://localhost:8080` (Default credentials: `airflow`/`airflow`)

### Step 2: Create Connection
1.  Navigate to **Admin** -> **Connections**.
2.  Click **+** to add a new connection.
3.  **Connection Id:** `snowflake_conn`
4.  **Connection Type:** `Snowflake`
5.  **Schema:** `RAW_DATA`
6.  **Login/Password/Account:** Enter your credentials.
7.  **Extra:**
    ```json
    {
      "account": "your_account_id",
      "warehouse": "COMPUTE_WH",
      "database": "AIRLINE_DB",
      "region": "us-east-1",
      "role": "ACCOUNTADMIN"
    }
    ```
8.  Save the connection. Now you can trigger the DAG `airline_main_pipeline`.