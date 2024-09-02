from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fpdf import FPDF
from profiler.config import EMAIL_PASSWORD, EMAIL_SENDER, SMTP_PORT, SMTP_SERVER


class DataValidator:
    def __init__(self, df: DataFrame):
        self.df = df
        self.results = []

    def add_result(self, validation_name: str, result: bool):
        """Store the result of each validation."""
        self.results.append((validation_name, result))

    def validate_column_exists(self, column_name: str) -> bool:
        result = column_name in self.df.columns
        self.add_result(f"Column Exists - {column_name}", result)
        return result

    def validate_data_type(self, column_name: str, expected_type: str) -> bool:
        actual_type = dict(self.df.dtypes).get(column_name)
        result = actual_type == expected_type
        self.add_result(f"Data Type - {column_name}", result)
        return result

    def validate_no_nulls(self, *columns: str) -> bool:
        null_counts = self.df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns])
        null_counts = null_counts.collect()[0]
        result = all(count == 0 for count in null_counts)
        self.add_result(f"No Nulls - {', '.join(columns)}", result)
        return result

    def validate_unique(self, *columns: str) -> bool:
        total_count = self.df.count()
        unique_count = self.df.select(*columns).distinct().count()
        result = total_count == unique_count
        self.add_result(f"Unique - {', '.join(columns)}", result)
        return result

    def validate_category_membership(self, column_name: str, valid_values: list) -> bool:
        invalid_count = self.df.filter(~F.col(column_name).isin(valid_values)).count()
        result = invalid_count == 0
        self.add_result(f"Category Membership - {column_name}", result)
        return result

    def validate_range(self, column_name: str, min_value, max_value) -> bool:
        """Check if values in a column fall within a specified range."""
        min_check = self.df.filter(F.col(column_name) < min_value).count() == 0
        max_check = self.df.filter(F.col(column_name) > max_value).count() == 0
        return min_check and max_check

    def validate_regex(self, column_name: str, regex_pattern: str) -> bool:
        """Check if values in a column match a specified regular expression pattern."""
        invalid_count = self.df.filter(~F.col(column_name).rlike(regex_pattern)).count()
        return invalid_count == 0

    def validate_min_length(self, column_name: str, min_length: int) -> bool:
        """Check if the length of values in a column is at least a certain minimum."""
        length_check = self.df.filter(F.length(F.col(column_name)) < min_length).count()
        return length_check == 0

    def validate_max_length(self, column_name: str, max_length: int) -> bool:
        """Check if the length of values in a column does not exceed a certain maximum."""
        length_check = self.df.filter(F.length(F.col(column_name)) > max_length).count()
        return length_check == 0

    def validate_cross_field(self, column_name1: str, column_name2: str, comparison: str) -> bool:
        if comparison == 'less_than':
            invalid_count = self.df.filter(F.col(column_name1) >= F.col(column_name2)).count()
        elif comparison == 'greater_than':
            invalid_count = self.df.filter(F.col(column_name1) <= F.col(column_name2)).count()
        elif comparison == 'equal_to':
            invalid_count = self.df.filter(F.col(column_name1) != F.col(column_name2)).count()
        else:
            raise ValueError("Invalid comparison type provided.")
        
        result = invalid_count == 0
        self.add_result(f"Cross Field Validation - {column_name1} {comparison} {column_name2}", result)
        return result

    def validate_custom(self, custom_function) -> bool:
        """Apply a custom validation function to the DataFrame."""
        return custom_function(self.df)

    def generate_report(self) -> str:
        """Generate a simple text-based report of the validation results."""
        report = "Data Validation Results:\n\n"
        for validation, result in self.results:
            status = "PASS" if result else "FAIL"
            report += f"{validation}: {status}\n"
        return report
    
    def send_email(self, report: str, emails: list):
        """Send the validation report via email."""
        sender_email = EMAIL_SENDER
        sender_password = EMAIL_PASSWORD
        
        # Email settings
        subject = "Data Validation Report"
        message = MIMEMultipart()
        message["From"] = sender_email
        message["Subject"] = subject
        
        # Add report as the email body
        message.attach(MIMEText(report, "plain"))
        
        # Send email to each recipient
        for email in emails:
            message["To"] = email
            text = message.as_string()
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, email, text)
        
        print("Emails sent successfully.")

    def save_pdf_report(self, pdf_path="validation_report.pdf"):
        """Save the validation results as a PDF."""
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        
        pdf.cell(200, 10, txt="Data Validation Results", ln=True, align="C")
        pdf.ln(10)
        
        for validation, result in self.results:
            status = "PASS" if result else "FAIL"
            pdf.cell(200, 10, txt=f"{validation}: {status}", ln=True)
        
        pdf.output(pdf_path)
        print(f"PDF report saved as {pdf_path}.")
