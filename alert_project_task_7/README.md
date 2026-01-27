# Log Analysis System

System for real-time processing and analysis of mobile application logs. The solution is designed to detect fatal errors.

## Features

* **Log Ingestion:** Efficiently reads large CSV log files using chunking.
* **Rule-Based Alerting:** Scalable architecture to apply multiple check rules.
* **Dockerized:** Runs with a single command in an isolated environment.

## Tech Stack & Design Choices

* **Python 3.9:** Reliable and universally supported version.
* **Pandas:** Used for high-performance data manipulation.
* **Docker:** Ensures the application runs consistently on any machine, eliminating "it works on my machine" issues.
* **Strategy Pattern:**
    * *Why?* The system uses a list of rule objects (`FatalErrorChecker`, `BundleFatalChecker`). This makes the system **Open for Extension, Closed for Modification**. To add a new rule, you simply create a new class and add it to the list, without rewriting the main loop.

## Dataset Setup

Due to the file size, the dataset is not included in the repository. You must download it manually before running the system.

1.  **Download the dataset** from this link: (https://drive.google.com/open?id=1FtL2NpnqGJbs35hxfsJTyFjPXTt7U_nH).
2.  If folder named `data` is not present in your root directory of the project, create it.
3.  Place the downloaded file inside the `data` folder.
4.  **Rename the file** to `data.csv` (if it has a different name).

## How to Run

### Prerequisites
* Docker & Docker Compose installed.

### Start the System
Run the following command in the project root:

```bash
docker-compose up --build