import os
import time
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import resource
from base_transformer import BaseEurostatTransformer
from data_validator import DataValidator

logger = logging.getLogger(__name__)


class EurostatChunkedTransformer(BaseEurostatTransformer):
    def __init__(self, dataset_name, chunk_size=50000):
        super().__init__(dataset_name)
        self.chunk_size = chunk_size
        self.output_data_file = os.path.join(self.BASE_DIR, "output", "processed",
                                             f"{self.dataset_name}_chunked_processed")

        self.parquet_writer = None

    # Unzip raw data, read it as TSV, replace symbols to NaN
    def read_raw_data_in_chunks(self):
        return pd.read_csv(
            self.input_data_file,
            chunksize=self.chunk_size,
            **self.get_read_options(),
        )

    # For each chunk, split dimension column, apply filters, clean, and converting data into a final format
    def process_single_chunk(self, chunk_df):
        df_work = self.split_dimension_column(chunk_df)
        df_work = self.apply_filters(df_work)

        if df_work.empty:
            return pd.DataFrame()

        df_work = self.wide_to_long_format(df_work)
        df_work = self.clean_values(df_work)
        df_work = self.rename_columns(df_work)
        df_work = self.add_derived_columns(df_work)

        return df_work

    # Save every chunk in Parquet format
    def save_chunk(self, df_chunk):

        if df_chunk.empty:
            return

        # Convert DF to PyArrow table to write Parquet
        try:
            table = pa.Table.from_pandas(
                df_chunk,
                preserve_index=False,
            )

            # For the first non-empty chunk, create the Parquet writer using its schema and Snappy compression
            if self.parquet_writer is None:
                self.parquet_writer = pq.ParquetWriter(
                    f"{self.output_data_file}.parquet",
                    table.schema,
                    compression="snappy",
                )

            # Write the current chunk to the Parquet file
            self.parquet_writer.write_table(table)

        except Exception as e:
            raise IOError(f"Failed to save chunk to disk: {e}")

    def run(self):
        if not self.raw_file_exists():
            raise FileNotFoundError(f"Raw input file not found: {self.input_data_file}")

        # Define validation rules based on the dataset name
        if self.dataset_name == "migr_asyappctzm":
            val_req_cols = ["time_period", "applicant_count", "country_code", "flag"]
            val_not_null = ["time_period", "applicant_count", "country_code"]
            val_metric = "applicant_count"
            val_time_col = "time_period"
            val_time_regex = r"\d{4}-(0[1-9]|1[0-2])"
        elif self.dataset_name == "migr_asydcfstq":
            val_req_cols = ["time_period", "decision_count", "country_code", "decision"]
            val_not_null = ["time_period", "decision_count", "country_code"]
            val_metric = "decision_count"
            val_time_col = "time_period"
            val_time_regex = r"\s*\d{4}-Q[1-4]\s*"
        elif self.dataset_name == "migr_asypenctzm":
            val_req_cols = ["time_period", "pending_count", "country_code"] 
            val_not_null = ["time_period", "pending_count", "country_code"]
            val_metric = "pending_count"
            val_time_col = "time_period"
            val_time_regex = r"\d{4}-(0[1-9]|1[0-2])" 
        else:
            raise ValueError(f"Unknown dataset for validation rules: {self.dataset_name}")

        
        logger.info(f"***** Starting Chunked pipeline, Chunk size: ({self.chunk_size}) .....")

        total_start_counter = time.perf_counter()

        total_read_time = 0.0
        total_processing_time = 0.0
        total_validation_time = 0.0
        total_write_time = 0.0
        total_rows_processed = 0
        chunk_count = 0

        # start counter for the FIRST chunk
        read_start_counter = time.perf_counter()
        # Chunks preparation
        chunks_generator = self.read_raw_data_in_chunks()

        try:
            for chunk in chunks_generator:
                chunk_count += 1

                ### 1- Calculate  Input Loading Time of each chunk ...
                read_end_counter = time.perf_counter()
                total_read_time += (read_end_counter - read_start_counter)

                ### 2- Calculate Processing time of each chunk ...
                logger.info(f"***** Start Processing Chunk: #{chunk_count} ...")
                # Calculate Processing time of each chunk and add it to total_processing_time
                transform_start_counter = time.perf_counter()
                processed_chunk = self.process_single_chunk(chunk)
                transform_end_counter = time.perf_counter()
                total_processing_time += (transform_end_counter - transform_start_counter)
                del chunk

                ### 3- Calculate Validation time of each chunk
                if not processed_chunk.empty:
                    validation_start_counter = time.perf_counter()

                    validator = DataValidator(processed_chunk)
                    validator.run_validation(
                    required_cols=val_req_cols,
                    not_null_cols=val_not_null,
                    metric_col=val_metric,
                    time_col=val_time_col,
                    time_regex=val_time_regex
)


                    validation_end_counter = time.perf_counter()
                    total_validation_time += (validation_end_counter - validation_start_counter)

                ### 4- Calculate Parquet Output Writing Time of each chunk
                if not processed_chunk.empty:

                    write_start_counter = time.perf_counter()

                    self.save_chunk(processed_chunk)

                    write_end_counter = time.perf_counter()
                    total_write_time += (write_end_counter - write_start_counter)

                    rows_in_chunk = len(processed_chunk)
                    total_rows_processed += rows_in_chunk

                    logger.info(f"Chunk number ({chunk_count}) saved and {rows_in_chunk} rows were added"
                        f"(Total rows so far: {total_rows_processed}).")

                else:
                    logger.info(f"Chunk number ({chunk_count}) resulted in 0 rows ")

                 # delete the processed chunk before loading the next one
                del processed_chunk

                read_start_counter = time.perf_counter()
        # Make sure to close parquet to not lose the data if anything happened
        finally:
            if self.parquet_writer is not None:
                parquet_close_start = time.perf_counter()

                self.parquet_writer.close()

                parquet_close_end = time.perf_counter()
                total_write_time += (parquet_close_end - parquet_close_start)

                self.parquet_writer = None
                logger.info("Parquet streaming writer closed")

        ### 5- Calculate the total time
        total_end_counter = time.perf_counter()
        total_time = total_end_counter - total_start_counter

        # Print final result
        logger.info("Chunked transformation finished successfully...")
        logger.info(f" Total Read Time: {total_read_time:.4f}s | Total Transform Time: {total_processing_time:.4f}s "
                    f"| Total Validation Time: {total_validation_time:.4f}s | Total Write Time: {total_write_time:.4f}s")
        logger.info(f" TOTAL TIME: {total_time:.4f}s ({total_time / 60:.2f} min)")

        ### Printing
        print("*******[CHUNKED RESULTS]*******", flush=True)
        print(f"1- Input Loading Time: {total_read_time:8.4f} sec  ({(total_read_time / total_time) * 100:5.1f}%)", flush=True)
        print(f"2- Data Processing Time   : {total_processing_time:8.4f} sec  ({(total_processing_time / total_time) * 100:5.1f}%) [Core Processing Metric]", flush=True)
        print(f"3- Data Validation Time  : {total_validation_time:8.4f} sec  ({(total_validation_time / total_time) * 100:5.1f}%) [Micro-batching Validation Overhead]", flush=True)
        print(f"4- Parquet Output Writing Time   : {total_write_time:8.4f} sec  ({(total_write_time / total_time) * 100:5.1f}%)", flush=True)
        print(f" TOTAL TIME    : {total_time:8.4f} sec  ({total_time / 60:.2f} min)", flush=True)
        print(
            f"Total Rows        : "
            f"{total_rows_processed}",
            flush=True,
        )
        peak_memory_mb = (
                resource.getrusage(
                    resource.RUSAGE_SELF
                ).ru_maxrss / 1024
        )
        print(
            f"Peak Memory Usage : "
            f"{peak_memory_mb:.2f} MB",
            flush=True,
        )

        return total_rows_processed