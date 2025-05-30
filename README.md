# Best Python Libraries for Data Engineering

This repository contains examples of various Python libraries and tools for data engineering tasks, including data extraction, transformation, validation, and loading.

## Setup Instructions

### Using UV (Recommended)

```bash
# install UV
uv venv --python python3.13

# activate your virtual env
source .venv/bin/activate

# install the requirements
uv pip install -r requirements.txt
```

### Using pip

```bash
# create a virtual environment
python -m venv .venv

# activate your virtual env
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# install the requirements
pip install -r requirements.txt
```

## Files and Examples

This repository includes the following examples:

### 1. Airbyte Data Extraction

- **1_pyairbyte_source.py**: Demonstrates how to use the Airbyte Python SDK to extract data from a source (source-faker). This example shows how to configure a source, check its configuration, and read data into a pandas DataFrame.

### 2. Prefect Workflow Orchestration with Airbyte

- **2_prefect+airbyte_demo.py**: Shows how to integrate Airbyte with Prefect for workflow orchestration. This example defines a complete data pipeline with extraction, transformation, analysis, and reporting tasks.

### 3. Data Cleansing with Pandas

- **3_pandas_cleansing.py**: Demonstrates data cleansing techniques using pandas. This example loads data from Airbyte, introduces artificial data issues, and then applies various cleansing techniques to address missing values, duplicates, outliers, and format standardization.

### 4. Data Validation with Great Expectations

- **4_greatexpectations_demo.py**: Shows how to use Great Expectations for data validation. This example creates an expectation suite, validates data against it, cleanses data based on validation results, and generates a data quality report.

### 5. Data Loading to Snowflake with Pandas

- **5_pandas_movement.py**: Demonstrates how to load data from a pandas DataFrame to Snowflake using the Snowflake connector's pandas integration. This example creates a sample DataFrame and uses `snowflake.connector.pandas_tools.write_pandas()` to efficiently write the data to Snowflake.

### 6. End-to-End Pipeline with Airbyte and Snowflake

- **6_pyairbyte_movement.py**: Shows an end-to-end data pipeline using Airbyte for extraction and loading to Snowflake. This example provides two methods for loading data to Snowflake: using Airbyte's native Snowflake destination and using the Snowflake connector directly.

## Dependencies

This project uses the following key libraries:

- **airbyte**: Python SDK for Airbyte, an open-source data integration platform
- **prefect**: Workflow orchestration tool for data pipelines
- **pandas**: Data manipulation and analysis library
- **numpy**: Numerical computing library
- **great_expectations**: Data validation and documentation framework
- **snowflake-connector-python**: Snowflake connector for Python with pandas integration

See `requirements.txt` for the complete list of dependencies and version requirements.

## Running the Examples

Each example can be run independently. For example:

```bash
# Run the Airbyte extraction example
python 1_pyairbyte_source.py

# Run the Prefect workflow example
python 2_prefect+airbyte_demo.py

# Run the pandas cleansing example
python 3_pandas_cleansing.py
```

Note that some examples (particularly those involving Snowflake) require additional credentials to be set as environment variables or provided during execution.
