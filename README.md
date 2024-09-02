# DataProfiler

**DataProfiler** is a Python package that performs comprehensive dataset profiling, testing, and validation using PySpark. The package is designed to help data engineers and data scientists quickly identify and address common data quality issues across large datasets.

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
df_csv = spark.read.csv("path_to_your_file.csv", header=True, inferSchema=True)

# Initialize the Profiler with the DataFrame
profile = Profiler(spark)
profile.load_data(df_csv)
```

### 2. Generating Summary Statistics

Generate and display summary statistics of your dataset:

```python
summary = profile.summary_statistics()
print(summary)
```

### 3. Validating the Dataset

Validate your dataset based on specific criteria:

```python
from profiler.validator import Validator

validator = Validator(profile)
validator.check_null_columns(["column1", "column2"])
validator.validate_data_type("column1", "integer")
```

### 4. Emailing Reports

After profiling and validation, you can generate a PDF report and send it via email:

```python
profile.email_report(recipients=["email1@example.com", "email2@example.com"])
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
├── main.py
├── README.md
├── setup.py
└── requirements.txt
```

## Contributing

Contributions are welcome! Please fork this repository and submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
