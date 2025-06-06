from setuptools import setup, find_packages

setup(
    name="DataLens",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "fpdf",
        "pandas",
        "numpy",
        "matplotlib",
        "seaborn",
    ],
    entry_points={
        "console_scripts": [
            "datalens=datalens.main:main",
        ],
    },
    author="McWilton Chikwenengere",
    author_email="mcwilton85@gmail.com",
    description="A package for dataset profiling, testing, and validation using PySpark",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/mcwilton/DataLens",  # Replace with your repository URL
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
