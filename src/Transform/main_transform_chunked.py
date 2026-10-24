import sys
import logging
import time
import os
import shutil
from transformer_chunked import EurostatChunkedTransformer

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
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    logger.info(f" Starting CHUNKED streaming transformation for dataset: {dataset_name} (Chunk size: {chunk_size})...")

    try:
        # Start counter
        start_time = time.perf_counter()

        transformer = EurostatChunkedTransformer(
            dataset_name, chunk_size=chunk_size
        )

        output_file = os.path.join(
            transformer.BASE_DIR,
            "output",
            "processed",
            f"{dataset_name}_chunked_processed.parquet",
        )

        source_file = os.path.join(
            transformer.BASE_DIR,
            "output",
            "last_download_date",
            f"{dataset_name}.txt",
        )

        processed_date_dir = os.path.join(
            transformer.BASE_DIR,
            "output",
            "last_processed_date",
        )

        destination_file = os.path.join(
            processed_date_dir,
            f"{dataset_name}.txt",
        )

        if not os.path.isfile(source_file):
            raise FileNotFoundError(
                f"Download date file not found: {source_file}"
            )

        with open(source_file, "r", encoding="utf-8") as file:
            source_date = file.read().strip()

        if not source_date or source_date.lower() in ("none", "null"):
            raise ValueError(
                f"Valid download date not found: {source_file}"
            )

        processed_date = None

        if os.path.isfile(destination_file):
            with open(destination_file, "r", encoding="utf-8") as file:
                processed_date = file.read().strip()

        if (
            os.path.isfile(output_file)
            and os.path.getsize(output_file) > 0
            and source_date == processed_date
        ):
            logger.info(
                "Dataset %s is already processed. Skipping transformation.",
                dataset_name,
            )
            return

        # Check the existing output before transformation
        previous_output_state = None

        if os.path.isfile(output_file):
            stat = os.stat(output_file)
            previous_output_state = (stat.st_mtime_ns, stat.st_size)

        # Run transformation
        transformer.run()

        # Make sure the output file exists and is not empty
        if not os.path.isfile(output_file) or os.path.getsize(output_file) == 0:
            raise RuntimeError(
                f"Transformation did not produce a valid Parquet file: {output_file}"
            )

        # Ensure this run actually updated the Parquet output
        stat = os.stat(output_file)
        current_output_state = (stat.st_mtime_ns, stat.st_size)

        if previous_output_state == current_output_state:
            raise RuntimeError(
                "The Parquet output was not updated by this transformation run."
            )

        # Copy the download date only after successful transformation
        os.makedirs(processed_date_dir, exist_ok=True)

        shutil.copy2(source_file, destination_file)

        logger.info(
            "Successfully updated last_processed_date for %s",
            dataset_name,
        )

        # End counter
        end_time = time.perf_counter()
        execution_time = end_time - start_time

        logger.info("Chunked streaming transform finished!")

        # Print the result
        print(
            f"[CHUNKED] Total Execution Time: {execution_time:.4f} seconds ({execution_time / 60:.2f} min)",
            flush=True
        )

    except Exception as e:
        logger.exception(f" Obs! Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()