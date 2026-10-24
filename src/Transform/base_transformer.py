import os
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Countries dictionary
GEO_NAME_MAP = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "CH": "Switzerland",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DE": "Germany",
    "DK": "Denmark",
    "EE": "Estonia",
    "EL": "Greece",
    "ES": "Spain",
    "EU27_2020": "European Union - 27 countries",
    "FI": "Finland",
    "FR": "France",
    "HR": "Croatia",
    "HU": "Hungary",
    "IE": "Ireland",
    "IS": "Iceland",
    "IT": "Italy",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "ME": "Montenegro",
    "MT": "Malta",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "UK": "United Kingdom",
}


METRIC_COLUMNS = {
    "migr_asyappctzm": "applicant_count",
    "migr_asydcfstq": "decision_count",
    "migr_asypenctzm": "pending_count",
}

            
class BaseEurostatTransformer:
    def __init__(self, dataset_name):
        if not dataset_name or not dataset_name.strip():
            raise ValueError("Eurostat Dataset name should not be empty!")

        # Get the project root directory
        self.BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        os.makedirs(os.path.join(self.BASE_DIR, "output", "processed"), exist_ok=True)

        self.dataset_name = dataset_name
        self.input_data_file = os.path.join(self.BASE_DIR, "output", "raw", f"{self.dataset_name}.tsv.gz")

        try:
            self.metric_col = METRIC_COLUMNS[self.dataset_name]
        except KeyError:
            raise ValueError(
                f"Unsupported dataset: {self.dataset_name}"
            ) from None

    
    def get_read_options(self):
        return {
            "sep": "\t",
            "compression": "gzip",
            "dtype": str,
            "keep_default_na": True,
            "na_values": [":", ": ", "–", "-"],
        }

    def raw_file_exists(self):
        return os.path.exists(self.input_data_file)


    # This function is to split the combined Eurostat dimension column into separate columns
    def split_dimension_column(self, df):
        dim_col = df.columns[0]
        # Cut off the name after \. FX, applicant,age,geo\TIME_PERIOD so it cuts TIME_PERIOD
        dim_part = dim_col.split("\\")[0]
        # Split by comma and clean spaces. FX, applicant,age,geo...
        dim_names = [col.strip() for col in dim_part.split(",")]
        # Split the stacked elements in the entire first column into separate columns
        dims = df[dim_col].astype(str).str.split(",", expand=True)
        # Assign the extracted real names to our new columns
        dims.columns = dim_names
        # Replace the old columns with the new ones we created
        df = pd.concat([dims, df.drop(columns=[dim_col])], axis=1)
        # Clean the extra spaces in headers
        df.columns = df.columns.str.strip()
        return df


    # Apply specific filters
    def apply_filters(self, df_wide):
        df_clean = df_wide.copy()

        # Include total sex and age only
        if "sex" in df_clean.columns:
            df_clean = df_clean[
                df_clean["sex"].astype(str).str.strip() == "T"
            ].copy()

        if "age" in df_clean.columns:
            df_clean = df_clean[
                df_clean["age"].astype(str).str.strip() == "TOTAL"
            ].copy()

        # Only keep known reporting geographies and EU27 total
        if "geo" in df_clean.columns:
            geo_clean = df_clean["geo"].astype("string").str.strip()
            allowed_geo = set(GEO_NAME_MAP.keys())

            # Detect unexpected codes
            observed_geo = set(geo_clean.dropna().unique())
            unexpected_geo = sorted(observed_geo - allowed_geo)

            if unexpected_geo:
                logger.warning(
                    "Unexpected geo codes excluded from processed data: %s",
                    unexpected_geo,
                )

            df_clean["geo"] = geo_clean
            df_clean = df_clean.loc[
                geo_clean.isin(allowed_geo)
            ].copy()

        return df_clean


    # This function is to check if a column name is a time period or not
    def is_time_column(self, col_name):
        # Get column name without any space
        col_name = str(col_name).strip()
        
        return bool(
        re.fullmatch(r"\d{4}", col_name)
        or re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", col_name)
        or re.fullmatch(r"\d{4}-Q[1-4]", col_name)
    )

    # This function is to convert the time periods from wide format to long format
    def wide_to_long_format(self, df):
        # Extract a list of all time-related columns
        time_cols = [col for col in df.columns if self.is_time_column(col)]
        # Take all columns that are not a time period
        id_vars = [col for col in df.columns if col not in time_cols]

        if not time_cols:
            raise ValueError("No time period columns were detected in the dataset")

        # Put the time periods in a new column called time_period, and the value for each
        # time_period put it in a new column called value_raw
        df_long = df.melt(
            id_vars=id_vars,
            value_vars=time_cols,
            var_name="time_period",
            value_name="value_raw"
        )
        return df_long

    # This function is to clean raw value column and convert it into usable numeric values
    def clean_values(self, df_long):
        df_long = df_long.copy()

        df_long["value_raw"] = (
        df_long["value_raw"]
        .astype("string")
        .str.strip()
    )

        # Replace missing-value markers
        df_long["value_raw"] = df_long["value_raw"].replace(
        r"^\s*[:–-]\s*$",
        pd.NA,
        regex=True,
    )

        df_long = df_long.dropna(
        subset=["value_raw"]
    ).copy()

        # Extract the numeric measurement
        numeric_values = df_long["value_raw"].str.extract(
        r"(-?\d+(?:\.\d+)?)",
        expand=False,
    )

        df_long[self.metric_col] = pd.to_numeric(
        numeric_values,
        errors="coerce",
    )

        # Keep the complete trailing alphabetical flag
        df_long["flag"] = df_long["value_raw"].str.extract(
        r"([a-z]+)$",
        expand=False,
    )

        df_long = df_long.dropna(
        subset=[self.metric_col]
    ).copy()

    # Remove the raw value after extracting the numeric metric and flag
        df_long = df_long.drop(columns=["value_raw"])

        return df_long


    def rename_columns(self, df_clean):
        rename_map = {}
        if "geo" in df_clean.columns:
            rename_map["geo"] = "country_code"
        if "citizen" in df_clean.columns:
            rename_map["citizen"] = "nationality_code"
        if "applicant" in df_clean.columns:
            rename_map["applicant"] = "applicant_type"
        if "age" in df_clean.columns:
            rename_map["age"] = "age_group"

        df_clean = df_clean.rename(columns=rename_map)
        return df_clean

    # This function translates short country codes into full names and marks EU totals.
    def add_derived_columns(self, df_clean):
        df_clean = df_clean.copy()

        if "country_code" in df_clean.columns:
            country_code_clean = (
                df_clean["country_code"]
                .astype("string")
                .str.strip()
            )

            df_clean["country_code"] = country_code_clean

            # Save the normalized country code
            df_clean["country_name"] = (
                country_code_clean
                .map(GEO_NAME_MAP)
                .fillna(country_code_clean)
            )
        return df_clean