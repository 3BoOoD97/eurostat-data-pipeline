import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, df):
        self.df = df
        if df.empty:
            raise ValueError("Transformed DataFrame is empty")

    # Check if required columns exist or not
    def validate_required_columns(self, required_columns):
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(
                f"Required columns: {missing_columns} are missing"
            )

    # Check if there are columns have null values
    def validate_no_nulls_in_columns(self, columns):
        null_columns = [
            col for col in columns
            if col in self.df.columns and self.df[col].isna().any()
        ]
        if null_columns:
            raise ValueError(
                f"The dataset contains null values in columns: {null_columns}"
            )

    # Check if there are negative values in "metric_value" column
    def validate_non_negative_metric(self, metric_column: str):
        if metric_column in self.df.columns:
            if (self.df[metric_column] < 0).any():
                raise ValueError(
                    f"The dataset contains negative values in '{metric_column}'"
                )

    # Validate that time_period is in YYYY-MM format with a valid month
    def validate_time_period_format(self, column_name: str, regex_pattern: str):
        if column_name not in self.df.columns:
            return

        invalid_values = self.df[
            ~self.df[column_name]
            .astype(str)
            .str.fullmatch(regex_pattern, na=False)  
        ][column_name].unique()

        if len(invalid_values) > 0:
            raise ValueError(
                f"The dataset contains invalid {column_name} values: "
                f"{list(invalid_values[:10])}"
            )


    def run_validation(self, required_cols, not_null_cols, metric_col, time_col, time_regex):
        logger.info("Starting data validation...")
        self.validate_required_columns(required_cols)
        self.validate_no_nulls_in_columns(not_null_cols)
        self.validate_non_negative_metric(metric_col)
        self.validate_time_period_format(time_col, time_regex)
        logger.info("Data validation completed successfully")
