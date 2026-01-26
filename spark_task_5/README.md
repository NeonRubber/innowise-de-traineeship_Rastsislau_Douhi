# LMS Task 5 - Spark

## Technology Stack
*   **Apache Spark** (PySpark)
*   **PostgreSQL 13**
*   **Docker & Docker Compose**
*   **JupyterLab** (as the execution environment)

## Project Structure
*   `docker-compose.yml` — Docker services configuration.
*   `db_init/` — SQL scripts for database initialization (Schema and Data).
*   `jars/` — JDBC driver for Spark to connect to PostgreSQL.
*   `work/` — Directory for Jupyter notebooks (`.ipynb`).

## Setup and Running

1.  **Clone the repository:**

2.  **Verify Driver:**
    Ensure that `postgresql-42.6.0.jar` is present in the `jars/` directory.

3.  **Start Containers:**
    Execute the command in the project root:
    ```bash
    docker-compose up -d
    ```

## How to Connect VS Code

If you plan on working with Jupyter notebooks in VS Code, you need to connect to the Jupyter server running inside your Docker container. This setup allows you to leverage VS Code's rich editing features while executing code in the containerized Spark environment.

### Step 1: Jupyter Server URL
The Jupyter server will always be accessible at:
`http://127.0.0.1:8888/lab?token=mysecrettoken`

### Step 2: Configure VS Code
1.  Open an `.ipynb` file from the `work/` folder in VS Code.
2.  In the top-right corner of the notebook editor, click on the **"Select Kernel"** button.
    *   *(It might also display your local Python version, e.g., "Python 3.10.0").*
3.  In the dropdown menu, select: **Existing Jupyter Server...**.
4.  Enter the URL: `http://127.0.0.1:8888/lab?token=mysecrettoken` (or your custom token) and press Enter.
5.  Select the kernel: **Python 3 (ipykernel)**.

Your code will now execute within the Docker container, gaining access to Spark and the database.

## Queries
This project contains queries in form of .ipynb file. Queries are made without using raw SQL code (as specified by task description in LMS).

1.  Output the number of movies in each category, sorted in descending order.
2.  Output the 10 actors whose movies rented the most, sorted in descending order.
3.  Output the category of movies on which the most money was spent.
4.  Output the names of movies that are not in the inventory.
5.  Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them.
6.  Output cities with the number of active and inactive customers (active - `customer.active = 1`). Sort by the number of inactive customers in descending order.
7.  Output the category of movies that have the highest number of total rental hours in cities (where `customer.address_id` is in that city) that start with the letter “a” and cities that contain a “-” symbol.
