# LMS Python task - Student & Room Data Pipeline

A containerized ETL pipeline designed to ingest JSON data into PostgreSQL, optimize database performance with indexes, and export analytical results into JSON or XML formats.

## Prerequisites

* **Docker & Docker Compose** installed on your machine.

## How to Run

### 1. Configuration
If it is not present in your folder, create a `.env` file in the root directory (or use default values in `docker-compose`):

```ini
DATABASE_NAME=student_db
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
DATABASE_HOST=db
DATABASE_PORT=5432
DB_MIN_CONN=1
DB_MAX_CONN=10
STUDENTS_FILE_PATH=/app/input_data/students_info.json
ROOMS_FILE_PATH=/app/input_data/rooms_info.json
EXPORT_PATH=/app/output_data/
EXPORT_DATA_FORMAT=json
```

### 2. Launch
Run the following command to build and start the pipeline:

```bash
docker-compose up --build
```

The pipeline will:

1.  Wait for the Database to be healthy.
2.  Create the schema (tables).
3.  Ingest Rooms and Students data (Batch Insert).
4.  Create Indexes for optimization.
5.  Execute analytical queries defined in `sql/queries.sql`.
6.  Export results to the `output_data/` directory.

### 3. Check Results
Results will be available in the local `output_data` folder:
* `rooms_with_students_counts.json`
* `five_rooms_with_the_smallest_average_age.json`
* *etc.*

### 4. Custom Execution (XML Export)
To run the pipeline again and export to XML:

```bash
docker-compose run --rm app --format xml
```

## SQL Optimization Strategy
Indexes are applied after the initial data load to avoid slowing down insertions:
* **idx_students_room**: Optimizes JOIN operations between Students and Rooms.
* **idx_students_birthday**: Speeds up age calculation and sorting.
* **idx_students_sex**: Optimizes filtering/grouping for mixed-sex room queries.
