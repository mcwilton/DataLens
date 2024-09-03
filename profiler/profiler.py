from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnan, count, when, lit, sum as spark_sum, mean as spark_mean, variance as spark_variance, stddev, skewness, kurtosis
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from fpdf import FPDF
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from profiler.config import EMAIL_PASSWORD, EMAIL_SENDER, SMTP_PORT, SMTP_SERVER


class Profiler:

    def __init__(self, spark: SparkSession, df: DataFrame = None):
        self.spark = spark
        self.df = df

    def load_data(self, df: DataFrame):
        """Load an existing DataFrame for profiling."""
        self.df = df

    def load_csv(self, file_path: str, header: bool = True, infer_schema: bool = True):
        """Load data from a CSV file into a DataFrame."""
        self.df = self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)

    def load_parquet(self, file_path: str) -> DataFrame:
        self.df = self.spark.read.parquet(file_path)
        return self.df

    def load_excel(self, file_path: str, sheet_name: str = "Sheet1") -> DataFrame:
        # To handle Excel files, you may need the Spark Excel library (com.crealytics.spark.excel)
        self.df = self.spark.read \
            .format("com.crealytics.spark.excel") \
            .option("sheetName", sheet_name) \
            .option("useHeader", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        return self.df
    
    def load_orc(self, file_path: str) -> DataFrame:
        self.df = self.spark.read.orc(file_path)
        return self.df

    # def check_outliers(self):
    #     outliers = {}
    #     for col in self.df.columns:
    #         quantiles = self.df.approxQuantile(col, [0.25, 0.75], 0.05)
    #         if quantiles and len(quantiles) == 2:
    #             iqr = quantiles[1] - quantiles[0]
    #             lower_bound = quantiles[0] - 1.5 * iqr
    #             upper_bound = quantiles[1] + 1.5 * iqr
    #             outliers[col] = self.df.filter((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).count()
    #     return outliers

    def check_outliers(self):
        if self.df is None:
            raise ValueError("No DataFrame loaded. Please load a dataset first.")
        
        outlier_summary = {}
        
        for column in self.df.columns:
            dtype = [field.dataType for field in self.df.schema.fields if field.name == column][0]

            # Check if column is numeric
            if isinstance(dtype, NumericType):
                quantiles = self.df.approxQuantile(column, [0.25, 0.75], 0.05)
                q1 = quantiles[0]
                q3 = quantiles[1]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                # Count outliers
                num_outliers = self.df.filter((col(column) < lower_bound) | (col(column) > upper_bound)).count()

                outlier_summary[column] = {
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "num_outliers": num_outliers
                }
            else:
                outlier_summary[column] = "Column is not numeric; skipping outlier detection."

        return outlier_summary
        
    def check_null_empty_values(self):
        nulls = self.df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in self.df.columns])
        empties = self.df.select([F.count(F.when(F.col(c) == "", c)).alias(c) for c in self.df.columns])
        return nulls, empties

    def check_wrong_dates(self, date_format="%Y-%m-%d"):
        wrong_dates = {}
        for col in self.df.columns:
            try:
                self.df = self.df.withColumn(col, F.to_date(F.col(col), date_format))
                wrong_dates[col] = self.df.filter(F.col(col).isNull()).count()
            except:
                continue
        return wrong_dates
    
    def check_incompleteness(self):
        incompleteness = {}
        for col in self.df.columns:
            count_nulls = self.df.filter(F.col(col).isNull()).count()
            count_empties = self.df.filter(F.col(col) == "").count()
            incompleteness[col] = count_nulls + count_empties
        return incompleteness
    
    def summary_statistics(self):
        """Generate summary statistics for the loaded DataFrame."""
        if self.df is None:
            raise ValueError("No DataFrame loaded. Please load a dataset first.")
        
        return self.df.describe().show()
    
    def profile_statistics(self):
        if self.df is None:
            raise ValueError("No DataFrame loaded. Please load a dataset first.")
        
        summary = {}
        
        for column in self.df.columns:
            col_data = self.df.select(column)

            # Sample size
            sample_size = self.df.count()

            # Null count
            null_count = col_data.filter(col(column).isNull()).count()

            # Null types and indices
            null_types = col_data.filter(col(column).isNull()).distinct().collect()
            null_types_index = {
                null_type[column]: col_data.filter(col(column).isNull()).rdd.zipWithIndex().filter(lambda x: x[0][column] == null_type[column]).map(lambda x: x[1]).collect()
                for null_type in null_types
            }

            # Data type representation
            data_type_representation = col_data.groupBy(col(column).cast("string")).count().withColumn("percentage", col("count")/sample_size * 100).select("percentage").collect()

            # Descriptive statistics
            numeric_col = col_data.select(column).where(col(column).cast("float").isNotNull())
            median_absolute_deviation = None # Need custom calculation
            sum_ = numeric_col.agg(spark_sum(col(column))).first()[0]
            mean_ = numeric_col.agg(spark_mean(col(column))).first()[0]
            variance_ = numeric_col.agg(spark_variance(col(column))).first()[0]
            stddev_ = numeric_col.agg(stddev(col(column))).first()[0]
            skewness_ = numeric_col.agg(skewness(col(column))).first()[0]
            kurtosis_ = numeric_col.agg(kurtosis(col(column))).first()[0]

            # Number of zeros
            num_zeros = numeric_col.filter(col(column) == 0).count()

            # Number of negatives
            num_negatives = numeric_col.filter(col(column) < 0).count()

            # Aggregate results
            summary[column] = {
                "sample_size": sample_size,
                "null_count": null_count,
                "null_types": null_types,
                "null_types_index": null_types_index,
                "data_type_representation": data_type_representation,
                "median_absolute_deviation": median_absolute_deviation,
                "sum": sum_,
                "mean": mean_,
                "variance": variance_,
                "stddev": stddev_,
                "skewness": skewness_,
                "kurtosis": kurtosis_,
                "num_zeros": num_zeros,
                "num_negatives": num_negatives,
            }
        
        return summary
    def missing_values(self):
        if self.df:
            return self.df.select([count(when(col(c).isNull(), c)).alias(c) for c in self.df.columns])
        else:
            raise ValueError("No DataFrame loaded. Please load a dataset first.")

    def data_distribution(self):
        if self.df:
            distributions = {}
            for col_name in self.df.columns:
                distributions[col_name] = self.df.groupBy(col_name).count().collect()
            return distributions
        else:
            raise ValueError("No DataFrame loaded. Please load a dataset first.")

    def check_duplicates(self):
        duplicates = self.df.groupBy(self.df.columns).count().filter(F.col("count") > 1)
        return duplicates

    def check_data_type_problems(self):
        type_issues = {}
        for col, dtype in self.df.dtypes:
            if dtype not in ['int', 'double', 'float', 'string', 'date']:
                type_issues[col] = dtype
        return type_issues

    def generate_pdf_report(self, output_file='profiling_report.pdf'):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt="Dataset Profiling Report", ln=True, align='C')

        sections = {
            "Skewness": self.check_skewness().collect(),
            "Outliers": self.check_outliers(),
            "Null/Empty Values": self.check_null_empty_values(),
            "Wrong Dates": self.check_wrong_dates(),
            "Incompleteness": self.check_incompleteness(),
            "Duplicates": self.check_duplicates().collect(),
            "Data Type Problems": self.check_data_type_problems(),
        }

        for section, result in sections.items():
            pdf.ln(10)
            pdf.cell(200, 10, txt=section, ln=True, align='L')
            pdf.ln(5)
            for item in result:
                pdf.multi_cell(0, 10, txt=str(item))

        pdf.output(output_file)
        return output_file

    def send_email_with_report(self, pdf_file, recipients):
        from_email = EMAIL_SENDER
        password = EMAIL_PASSWORD

        message = MIMEMultipart()
        message['From'] = from_email
        message['Subject'] = "Dataset Profiling Report"

        body = "Please find attached the dataset profiling report."
        message.attach(MIMEText(body, 'plain'))

        attachment = open(pdf_file, "rb")
        mimeBase = MIMEBase('application', 'octet-stream')
        mimeBase.set_payload((attachment).read())
        encoders.encode_base64(mimeBase)
        mimeBase.add_header('Content-Disposition', f"attachment; filename= {pdf_file}")
        message.attach(mimeBase)

        for recipient in recipients:
            message['To'] = recipient
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()
            server.login(from_email, password)
            text = message.as_string()
            server.sendmail(from_email, recipient, text)
            server.quit()


# pypi-AgEIcHlwaS5vcmcCJDA3ZTk3ZWQzLTE1YjItNGQ0Ny05ZTg3LWI5ZTczYzFlZGFkNwACKlszLCJmODgwODM4Yy0zNTNhLTRkYzEtOWE0NS1kMjkyN2U2NDU3MTYiXQAABiBDb8Y0f6azgdmDlIJsR5oegiN_JyiCGpw_z3ffVrJhHw