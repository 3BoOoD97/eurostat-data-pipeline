import os
import logging
import gzip


logger = logging.getLogger(__name__)


class FileManager:
    def __init__(self, dataset_name: str = None):
        if not dataset_name or not dataset_name.strip():
            raise ValueError("Dataset name should not be empty!!")

        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

        # Create folders to save date and raw data if they don't exist
        os.makedirs(os.path.join(BASE_DIR, "output", "raw"), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, "output", "last_download_date"), exist_ok=True)

        self.dataset_name = dataset_name
        self.output_gz_data_path = os.path.join(BASE_DIR, "output", "raw", f"{self.dataset_name}.tsv.gz")
        self.last_download_file_date = os.path.join(BASE_DIR, "output", "last_download_date",
                                                    f"{self.dataset_name}.txt")

    # This function checks if the data file exists locally or not
    def data_file_exists(self):
        return os.path.exists(self.output_gz_data_path)

    # This function saves the stream coming from Eurostat locally as a .gz file.
    def save_file(self, stream):
        temp_path = self.output_gz_data_path + ".part"

        try:
        # Download to a temporary file first
            with open(temp_path, "wb") as f:
                for chunk in stream.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            # Reject empty downloads
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                raise ValueError("Downloaded data file is empty")

            # Validate the complete GZIP file
            with gzip.open(temp_path, "rb") as gz:
                while gz.read(1024 * 1024):
                    pass

            # Replace the existing file only after successful validation
            os.replace(temp_path, self.output_gz_data_path)

            logger.info(f"Raw data file saved and verified: {self.output_gz_data_path}")

        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    logger.exception("Failed to remove temporary download file")

            raise IOError(f"Download failed or file validation failed: {e}") from e

        finally:
            stream.close()

    # This function returns the size of the local data file in MB if it exists
    def get_data_file_size_mb(self):
        if not self.data_file_exists():
            return 0
        return os.path.getsize(self.output_gz_data_path) / (1024 * 1024)

    # This function receives a date as a parameter and saves it as a text file.
    def save_last_download_date(self, last_update):
        # Save the passed date in a txt file
        try:
            with open(self.last_download_file_date, 'w', encoding='utf-8') as f:
                f.write(str(last_update))
            logger.info(f"Last download date saved: {self.last_download_file_date}")

        except Exception as e:
            raise IOError(f"Error while saving the last download date: {e}")

    # This function retrieves the date from the date txt file
    def read_last_download_date(self):
        # If there is date txt file
        if not os.path.exists(self.last_download_file_date):
            return None

        # Read the date and return it
        try:
            with open(self.last_download_file_date, 'r', encoding='utf-8') as f:
                date_str = f.read().strip()
                return date_str
        except Exception as e:
            raise IOError(f"Error while reading the last download date: {e}")
