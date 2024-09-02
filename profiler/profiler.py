from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when, col, count
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

    def check_outliers(self):
        outliers = {}
        for col in self.df.columns:
            quantiles = self.df.approxQuantile(col, [0.25, 0.75], 0.05)
            if quantiles and len(quantiles) == 2:
                iqr = quantiles[1] - quantiles[0]
                lower_bound = quantiles[0] - 1.5 * iqr
                upper_bound = quantiles[1] + 1.5 * iqr
                outliers[col] = self.df.filter((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).count()
        return outliers
    
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