# eurostat-data-pipeline

## Project Overview
The core idea is to build an automated, cloud-based data engineering pipeline that uses Python and Docker to extract, clean, validate, and process European asylum statistics from Eurostat. 
The pipeline is designed to run on a schedule in Azure and deliver processed Parquet files to a Microsoft Fabric Lakehouse.
Using monthly asylum applications, monthly pending applications, and quarterly asylum decisions, the platform will support analysis of asylum trends, pending cases, and decision outcomes. 
The resulting datasets will enable country-level comparisons against the EU27 aggregate and provide a foundation for Power BI dashboards and time-series forecasting to explore future application trends and potential pressures on asylum systems.

---

## Project Goals

- Build an automated data pipeline to collect and process European asylum statistics from Eurostat using Python and Docker.
- Run the pipeline on a schedule in Azure and send processed Parquet files to Microsoft Fabric.
- Ensure data quality and prepare reliable datasets for analysis.
- Analyze asylum applications, pending cases, and decision outcomes across European countries, comparing country-level trends with the EU27 aggregate.
- Build Power BI dashboards and explore time-series forecasting to identify trends and potential pressures on asylum systems.

---

## Datasets

| Dataset ID | Description | Frequency |
| :--- | :--- | :--- |
| [migr_asyappctzm](https://ec.europa.eu/eurostat/databrowser/view/migr_asyappctzm/default/table) | Asylum applications | Monthly |
| [migr_asypenctzm](https://ec.europa.eu/eurostat/databrowser/view/migr_asypenctzm/default/table) | Pending asylum applications | Monthly |
| [migr_asydcfstq](https://ec.europa.eu/eurostat/databrowser/view/migr_asydcfstq/default/table) | Asylum decisions | Quarterly |


## Architecture

### Local Architecture
The local pipeline uses Docker containers for extraction and transformation, with Airflow orchestrating the tasks and their dependencies

![Current-State Architecture](docs/current_local_architecture.jpeg)

### Azure Architecture
...

---

## Pipeline Flow
### 1. Extract Step
The `extract_pipeline` task runs the Dockerized extract container for the selected Eurostat dataset.

The Extractor container: 
- Validates the dataset name and handles empty/null input
- Retrieves the latest update date from Eurostat
- Compares the latest update date with the locally stored date:
* If there is no locally stored date -> Downloads the raw dataset in archived `.tsv.gz` format -> Save the update date from Eurostat locally -> Outputs an update status as the final line (UPDATED=true)
* If the locally stored date is older than the latest Eurostat update date -> Downloads the raw dataset in archived `.tsv.gz` format -> Save the update date from Eurostat locally -> Outputs an update status as the final line (UPDATED=true)
* If the locally stored date equals the latest Eurostat update date -> Skip the download -> Outputs an update status as the final line (UPDATED=false)
* Prints the update status as the final output line for Airflow to retrieve through XCom.

| Condition | Action | Output |
|---|---|---|
| No locally stored update date exists | Download the dataset and save the update date after a successful download. | `UPDATED=true` |
| The stored date is older than Eurostat’s latest update date | Download the updated dataset and save the new update date after a successful download. | `UPDATED=true` |
| The stored date matches Eurostat’s latest update date | Skip the download. | `UPDATED=false` |

Airflow uses the UPDATED status to determine whether the Transform step should run.

### 2. Update Check 
This task reads the extract status from XCom (UPDATED=false/UPDATED=true) and checks whether the processed CSV and Parquet files already exist, as follows:
* If UPDATED=true -> perform transform_pipeline task
* If the processed output files are missing -> perform transform_pipeline task
* If UPDATED=false and processed output files exist -> Skip transform_pipeline task

### 3. Transform Step
The transform_pipeline task runs the Dockerized transform container.
During this step, the container:
- Reads the raw .tsv.gz dataset that was downloaded from extract_pipeline
- Splits Eurostat dimension columns
- Converts the dataset from wide format to long format
- Cleans numeric values and extracts Eurostat flags
- Applies specific filtering
- Adds country names
- Runs validation checks using the `DataValidator` class to verify that required columns exist, specified columns do not contain null values, metric values are non-negative, and the time period follows the expected format.
- Renames the numeric metric column according to the dataset:
  - `applicant_count`: Number of asylum applications.
  - `pending_count`: Number of pending asylum applications.
  - `decision_count`: Number of asylum decisions.
  Each dataset uses its corresponding metric column to represent the measured values.
- Saves the transformed output as CSV and Parquet

### 4. Skip Step
If the dataset is already up to date and the processed output files exist, Airflow runs the skip_transform task.

### Pipeline Flow Summary Diagram
![Pipeline Flow Summary Diagram](docs/Airflow_diagram.jpeg)

---

## Local Pipeline Status

The local Docker and Airflow pipeline has been tested successfully with the following scenarios:

- First run with missing raw, processed, and metadata files
- No update with existing processed files
- Missing processed output files
- Missing raw data file
- Missing last download date file
- Invalid dataset handling
- Standalone Docker container execution
- Airflow DAG execution for all configured datasets

The local pipeline is stable for local execution. The next step is to implement and test cloud storage integration.

---

## Azure Deployment
The pipeline has been deployed to Azure using Azure Container Apps Jobs and a Docker image hosted in GitHub Container Registry.

### Azure Resources
| Resource | Name |
| :--- | :--- |
| **Resource Group** | `rg-eurostat-pipeline-dev` |
| **Container Apps Environment** | `cae-eurostat-pipeline-wp` |
| **Container Apps Job** | `eurostat-pipeline-job` |
| **Storage Account** | `eurostatpipe0255` |
| **Azure File Share** | `eurostat-output` |
| **Container Image** | `ghcr.io/3boood97/eurostat-pipeline:v0.1.0` |


### Container Execution
The Azure Job uses the Docker image from GitHub Container Registry.
The image contains the Python extraction and chunked transformation components, along with their required dependencies.
The pipeline orchestrator, src/main_pipeline.py, runs extraction first and transformation second for the selected dataset. 

### Storage
The storage contains the following directories:
| Directory | Purpose |
| :--- | :--- |
| `raw` | Downloaded source datasets |
| `processed` | Processed Parquet datasets |
| `last_download_date` | Stored source download dates |
| `last_processed_date` | Stored processing dates |

---

## Dashboard Visuals

### Executive Summary Report
Executive summary and key general indicators.
![Executive Summary Report](docs/BI%20Reports/Executive%20Summary.png)

### Applications Report
Analyzing the asylum trends and the distinction between first-time and SSEQ (Subsequent Applicant)
![Applications Report](docs/BI%20Reports/Applications.png)

### Decisions & Recognition Rates Report
Asylum decisions and recognition rates
![Decisions & Recognition Rates](<docs/BI Reports/Decisions & Recognition Rates.png>)

### Pending Report
Pending cases and a snapshot of the most recent monthly records.
![Pending Report](docs/BI%20Reports/Pending.png)

