import sys
import logging
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        logger.error("You must provide a dataset name as an argument!")
        sys.exit(1)

    dataset_name = sys.argv[1]
    chunk_size = sys.argv[2] if len(sys.argv) > 2 else "50000"

    steps = [
        (
            "Extract",
            [
                sys.executable,
                "src/Extract/main_extract.py",
                dataset_name,
            ],
        ),
        (
            "Transform",
            [
                sys.executable,
                "src/Transform/main_transform_chunked.py",
                dataset_name,
                chunk_size,
            ],
        ),
    ]

    try:
        for step_name, command in steps:
            logger.info(
                "Starting %s step for dataset %s",
                step_name,
                dataset_name,
            )

            subprocess.run(command, check=True)

            logger.info("%s step completed successfully", step_name)

        logger.info("Pipeline completed successfully")

    except subprocess.CalledProcessError as e:
        logger.error(
            "Pipeline step failed with exit code %s",
            e.returncode,
        )
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()