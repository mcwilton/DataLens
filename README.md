# DataProfiler

**DataProfiler** is a Python package that performs comprehensive dataset profiling and validation using PySpark. The package is designed to help data engineers and data scientists quickly identify and address common data quality issues across large datasets to help them process datasets effectively.

## Features

- **Dataset Profiling**: Automatically generate summary statistics, check for skewness, outliers, null values, and more.
- **Data Validation**: Validate datasets to ensure they meet specific quality criteria, such as no null values in key columns or correct data types.
- **Data Formats Supported**: Works with CSV, Parquet, Excel, and ORC file formats.
- **Email Reporting**: Generates a PDF report of profiling and validation results and sends it via email to multiple recipients.

## Installation

To install the package, use pip:

```bash
pip install dataprofiler
```

## Usage

### 1. Initializing the Profiler

Start by initializing a Spark session and loading your dataset:

```python
from pyspark.sql import SparkSession
from profiler.profiler import Profiler

spark = SparkSession.builder.appName("DataProfilerApp").getOrCreate()

# Load a dataset (e.g., CSV file)
df_csv = spark.read.csv("path_to_your_file/filename.csv", header=True, inferSchema=True)
 
# Load a Parquet file
# df_parquet = spark.load_parquet("path/to/your.parquet")

# Load an Excel file
# df_excel = spark.load_excel("path/to/your.xlsx")

# Load an ORC file
# df_orc = spark.load_orc("path/to/your.orc")

# Initialize the Profiler with the DataFrame
profile = Profiler(spark)
profile.load_data(df_csv)
```

### 2. Profiling the Dataset

Profile the dataset on any criteria:

```python
profile.summary_statistics()
profile.check_outliers()
profile.data_distribution(truncate=False)
profile.missing_values()
profile.generate_pdf_report()
profile.check_duplicates()
profile.check_data_type_problems()
profile.check_incompleteness()
profile.check_null_empty_values()
profile.check_wrong_dates()
profile.missing_values()
```

### 3. Validating the Dataset

Validate your dataset based on specific criteria:

```python
from profiler.validator import Validator

validator = Validator(profile)
validator.check_null_columns(["column1", "column2"])
validator.validate_data_type("column1", "integer")
validator.validate_column_exists("column")
validator.validate_no_nulls("column", "column2", "column3")
validator.validate_unique("column", "date")
validator.validate_category_membership("column", ["Active", "Inactive", "Pending"])
validator.validate_cross_field("start_date", "end_date", "less_than")

```

### 4. Emailing Reports

After profiling and/or validation, you can generate a PDF report and send it via email:

```python
# Send the report via email
# recipients = ["person1@example.com", "person2@example.com", "person3@example.com"]
# profile.send_email_with_report(pdf_file, recipients)
```

## Configuration

All configurations such as email settings, Spark session parameters, and file paths are managed through a configuration file (`config.yml`). Customize the `config.yml` file to suit your environment.

## Project Structure

```
DataProfiler/
│
├── profiler/
│   ├── __init__.py
│   ├── profiler.py
│   ├── validator.py
│
├── config/
│   ├── config.yml
│
├── README.md
├── setup.py
└── requirements.txt
```

## Contributing

Contributions are welcome! Please fork this repository and submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
