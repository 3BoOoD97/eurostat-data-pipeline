import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator

OUTPUT_PATH = os.getenv(
    "EUROSTAT_OUTPUT_VOLUME",
    "/run/desktop/mnt/host/c/Users/Hp/Desktop/eurostat-data-pipeline/output:/app/output"
)

OUTPUT_DIR = "/opt/airflow/output"


DATASETS_CONFIG = {
    "migr_asyappctzm": { 
        "schedule": "0 0 1,15 * *",
        "description": "Monthly Asylum Applications",
        "tags": ["eurostat", "monthly", "asylum"]
    },
    "migr_asypenctzm": { 
        "schedule": "0 0 1,15 * *",
        "description": "Monthly Pending Asylum Applications",
        "tags": ["eurostat", "monthly", "asylum"]
    },
    "migr_asydcfstq": { 
        "schedule": "0 0 1 * *",
        "description": "Quarterly Asylum Decisions",
        "tags": ["eurostat", "quarterly", "asylum"]
    }
}


def read_update_date(file_path):
    if not os.path.isfile(file_path):
        return None

    with open(file_path, "r", encoding="utf-8") as file:
        date_value = file.read().strip()

    if not date_value or date_value.lower() in {"none", "null"}:
        return None

    return date_value


# Function to check if the data needs update or not
def decide_if_transform(dataset_name, **kwargs):
    ti = kwargs["ti"]

    # Pull the last line of the output command
    status_output = ti.xcom_pull(task_ids="extract_pipeline")
    status_lines = str(status_output or "").strip().splitlines()
    status = status_lines[-1].strip() if status_lines else ""

    processed_parquet = os.path.join(
        OUTPUT_DIR,
        "processed",
        f"{dataset_name}_chunked_processed.parquet",
    )

    if status == "UPDATED=true":
        return "transform_pipeline"

    if status != "UPDATED=false":
        raise ValueError(
            f"Unexpected extraction status for {dataset_name}: {status!r}"
        )

    if (
        not os.path.isfile(processed_parquet)
        or os.path.getsize(processed_parquet) == 0
    ):
        return "transform_pipeline"

    downloaded_date = read_update_date(
        os.path.join(
            OUTPUT_DIR,
            "last_download_date",
            f"{dataset_name}.txt",
        )
    )

    processed_date = read_update_date(
        os.path.join(
            OUTPUT_DIR,
            "last_processed_date",
            f"{dataset_name}.txt",
        )
    )

    if not downloaded_date or downloaded_date != processed_date:
        return "transform_pipeline"

    return "skip_transform"


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Loop through all datasets in the configuration dictionary
for dataset_name, config in DATASETS_CONFIG.items():
    with DAG(
            dag_id=f"eurostat_{dataset_name}",
            default_args=DEFAULT_ARGS,
            start_date=datetime(2026, 1, 1),
            schedule=config["schedule"],
            catchup=False,
            max_active_runs=1,
            tags=config["tags"],
            description=config["description"],
    ) as dag:

        extract_pipeline = BashOperator(
            task_id="extract_pipeline",
            bash_command=(
                f'docker run --rm -v "{OUTPUT_PATH}" '
                f"eurostat-extract {dataset_name}"
            ),
            do_xcom_push=True, # To save the last line of the output to xcom
        )

        transform_pipeline = BashOperator(
            task_id="transform_pipeline",
            bash_command=(
                f'docker run --rm -v "{OUTPUT_PATH}" '
                f"eurostat-transform {dataset_name}"
            ),
        )

        skip_transform = EmptyOperator(
            task_id="skip_transform",
        )

        check_update = BranchPythonOperator(
            task_id="check_update",
            op_kwargs={"dataset_name": dataset_name},
            python_callable=decide_if_transform,
        )

        extract_pipeline >> check_update >> [
            transform_pipeline,
            skip_transform,
        ]

    globals()[dag.dag_id] = dag