import pandas as pd
import numpy as np
import airbyte as ab
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.profile import BasicSuiteBuilderProfiler
import json
from datetime import datetime

def load_data_from_airbyte(count=50_000, seed=123):
    """
    Load data from Airbyte using the source-faker connector.
    
    Args:
        count: Number of records to generate
        seed: Random seed for reproducibility
        
    Returns:
        DataFrame containing products data from Airbyte
    """
    print("Loading data from Airbyte...")
    
    # Create and install the source
    source: ab.Source = ab.get_source("source-faker")
    
    # Configure the source
    source.set_config(
        config={
            "count": count,
            "seed": seed,
        },
    )
    
    # Verify the config and creds by running `check`
    check_result = source.check()
    print(f"Source check result: {check_result}")
    
    # Select all of the source's streams and read data into the internal cache
    source.select_all_streams()
    read_result: ab.ReadResult = source.read()
    
    # Transform the loaded data to a pandas DataFrame
    products_df = read_result["products"].to_pandas()
    
    print(f"Successfully loaded {len(products_df)} products from Airbyte")
    
    # Introduce some artificial data issues for demonstration purposes
    # Note: In a real scenario, your data might already have these issues
    df = products_df.copy()
    
    # Introduce some NaN values (5% of the data)
    rows_to_modify = np.random.choice(df.index, size=int(len(df) * 0.05), replace=False)
    columns_to_modify = np.random.choice(df.columns, size=min(3, len(df.columns)), replace=False)
    
    for col in columns_to_modify:
        df.loc[np.random.choice(rows_to_modify, size=int(len(rows_to_modify) * 0.7), replace=False), col] = np.nan
    
    # Introduce some duplicates (create a few duplicate product IDs)
    if 'id' in df.columns:
        duplicate_rows = np.random.choice(df.index, size=min(10, int(len(df) * 0.01)), replace=False)
        original_rows = np.random.choice(df.index, size=len(duplicate_rows), replace=False)
        
        for i, (dup_idx, orig_idx) in enumerate(zip(duplicate_rows, original_rows)):
            if dup_idx != orig_idx:
                df.loc[dup_idx, 'id'] = df.loc[orig_idx, 'id']
    
    # Introduce some outliers in numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if len(df[col].dropna()) > 0:
            # Calculate the mean and std for the column
            col_mean = df[col].mean()
            col_std = df[col].std()
            
            # Create outliers at 5 standard deviations from the mean
            outlier_rows = np.random.choice(df.index, size=min(5, int(len(df) * 0.005)), replace=False)
            df.loc[outlier_rows, col] = col_mean + (5 * col_std * np.random.choice([-1, 1], size=len(outlier_rows)))
    
    print(f"Added artificial data issues for demonstration purposes:")
    print(f"- Missing values in {len(columns_to_modify)} columns")
    if 'id' in df.columns:
        print(f"- {len(duplicate_rows)} duplicate product IDs")
    print(f"- Outliers in numeric columns")
    
    return df

def create_expectation_suite():
    """
    Create an expectation suite for product data validation.
    
    Returns:
        ExpectationSuite: A Great Expectations suite with expectations for product data
    """
    print("\n" + "="*50)
    print("CREATING EXPECTATION SUITE")
    print("="*50)
    
    # Create a new expectation suite
    suite = ExpectationSuite(expectation_suite_name="product_data_suite")
    
    # Add expectations for product_id
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "product_id"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "product_id"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "product_id"}
        )
    )
    
    # Add expectations for name
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "name"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "name"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_value_lengths_to_be_between",
            kwargs={"column": "name", "min_value": 1, "max_value": 100}
        )
    )
    
    # Add expectations for price
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "price"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "price"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "price", "min_value": 0, "max_value": None}
        )
    )
    
    # Add expectations for stock
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "stock"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "stock", "min_value": 0, "max_value": None}
        )
    )
    
    # Add expectations for rating
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "rating"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "rating", "min_value": 1, "max_value": 5}
        )
    )
    
    # Add expectations for category
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "category"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "category"}
        )
    )
    
    print(f"Created expectation suite with {len(suite.expectations)} expectations")
    
    return suite

def validate_data_with_great_expectations(df, suite=None):
    """
    Validate the data using Great Expectations.
    
    Args:
        df: DataFrame to validate
        suite: ExpectationSuite to use for validation (if None, a new one will be created)
        
    Returns:
        tuple: (validated_df, validation_result)
    """
    print("\n" + "="*50)
    print("VALIDATING DATA WITH GREAT EXPECTATIONS")
    print("="*50)
    
    # Convert the pandas DataFrame to a Great Expectations dataset
    ge_df = ge.from_pandas(df)
    
    if suite is None:
        # Create a new suite
        suite = create_expectation_suite()
    
    # Validate the data against the suite
    validation_result = ge_df.validate(expectation_suite=suite, result_format="COMPLETE")
    
    # Print validation summary
    success = validation_result.success
    results_dict = validation_result.to_json_dict()
    
    print(f"\nValidation {'succeeded' if success else 'failed'}")
    print(f"Success rate: {results_dict['statistics']['success_percent']:.2f}%")
    print(f"Successful expectations: {results_dict['statistics']['successful_expectations']}")
    print(f"Evaluated expectations: {results_dict['statistics']['evaluated_expectations']}")
    
    # Print details of failed expectations
    if not success:
        print("\nFailed Expectations:")
        for result in results_dict["results"]:
            if not result["success"]:
                print(f"- {result['expectation_config']['expectation_type']} "
                      f"for {result['expectation_config']['kwargs'].get('column', 'N/A')}: "
                      f"{result.get('exception_info', {}).get('exception_message', 'Failed')}")
    
    return ge_df, validation_result

def profile_and_validate_data(df):
    """
    Profile the data and create a suite based on the profile, then validate.
    
    Args:
        df: DataFrame to profile and validate
        
    Returns:
        tuple: (ge_df, validation_result, profiled_suite)
    """
    print("\n" + "="*50)
    print("PROFILING DATA AND CREATING EXPECTATIONS")
    print("="*50)
    
    # Convert to Great Expectations dataset
    ge_df = ge.from_pandas(df)
    
    # Use the profiler to automatically create expectations
    print("Profiling data to automatically generate expectations...")
    profiler = BasicSuiteBuilderProfiler()
    profiled_suite, _ = profiler.profile(ge_df)
    
    print(f"Profiler created {len(profiled_suite.expectations)} expectations based on data patterns")
    
    # Add some custom expectations that the profiler might not have created
    print("Adding custom expectations...")
    
    # Ensure product_id is unique
    if not any(exp.expectation_type == "expect_column_values_to_be_unique" and 
               exp.kwargs.get("column") == "product_id" 
               for exp in profiled_suite.expectations):
        profiled_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "product_id"}
            )
        )
    
    # Ensure price is positive
    if not any(exp.expectation_type == "expect_column_values_to_be_between" and 
               exp.kwargs.get("column") == "price" and
               exp.kwargs.get("min_value") == 0
               for exp in profiled_suite.expectations):
        profiled_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "price", "min_value": 0, "max_value": None}
            )
        )
    
    # Validate against the profiled suite
    print("\nValidating data against profiled expectations...")
    validation_result = ge_df.validate(expectation_suite=profiled_suite, result_format="COMPLETE")
    
    # Print validation summary
    success = validation_result.success
    results_dict = validation_result.to_json_dict()
    
    print(f"\nValidation {'succeeded' if success else 'failed'}")
    print(f"Success rate: {results_dict['statistics']['success_percent']:.2f}%")
    print(f"Successful expectations: {results_dict['statistics']['successful_expectations']}")
    print(f"Evaluated expectations: {results_dict['statistics']['evaluated_expectations']}")
    
    return ge_df, validation_result, profiled_suite

def cleanse_data_based_on_validation(df, validation_result):
    """
    Cleanse the data based on validation results from Great Expectations.
    
    Args:
        df: Original DataFrame
        validation_result: Validation result from Great Expectations
        
    Returns:
        DataFrame: Cleansed DataFrame
    """
    print("\n" + "="*50)
    print("CLEANSING DATA BASED ON VALIDATION RESULTS")
    print("="*50)
    
    # Make a copy to avoid modifying the original
    clean_df = df.copy()
    
    # Extract failed expectations from validation results
    results_dict = validation_result.to_json_dict()
    failed_expectations = [result for result in results_dict["results"] if not result["success"]]
    
    print(f"Found {len(failed_expectations)} failed expectations to address")
    
    # Process each failed expectation
    for failed in failed_expectations:
        expectation_type = failed["expectation_config"]["expectation_type"]
        kwargs = failed["expectation_config"]["kwargs"]
        column = kwargs.get("column")
        
        if column is None:
            continue  # Skip expectations that don't target a specific column
        
        print(f"Addressing failed expectation: {expectation_type} for column {column}")
        
        # Handle different types of expectation failures
        if expectation_type == "expect_column_values_to_not_be_null":
            # Handle missing values
            if column == "product_id" or column == "name" or column == "category":
                # For critical columns, drop rows with missing values
                before_count = len(clean_df)
                clean_df = clean_df.dropna(subset=[column])
                print(f"  Dropped {before_count - len(clean_df)} rows with missing {column}")
            elif column == "price":
                # For price, fill with median
                clean_df[column] = clean_df[column].fillna(clean_df[column].median())
                print(f"  Filled missing {column} values with median")
            elif column == "stock":
                # For stock, fill with 0
                clean_df[column] = clean_df[column].fillna(0)
                print(f"  Filled missing {column} values with 0")
            elif column == "rating":
                # For rating, fill with median
                clean_df[column] = clean_df[column].fillna(clean_df[column].median())
                print(f"  Filled missing {column} values with median")
            else:
                # For other columns, fill with appropriate defaults
                if clean_df[column].dtype == 'object':
                    clean_df[column] = clean_df[column].fillna("")
                    print(f"  Filled missing {column} values with empty string")
                else:
                    clean_df[column] = clean_df[column].fillna(0)
                    print(f"  Filled missing {column} values with 0")
        
        elif expectation_type == "expect_column_values_to_be_unique":
            # Handle duplicates
            if column == "product_id":
                before_count = len(clean_df)
                clean_df = clean_df.drop_duplicates(subset=[column], keep='first')
                print(f"  Removed {before_count - len(clean_df)} duplicate {column} values")
        
        elif expectation_type == "expect_column_values_to_be_between":
            # Handle out-of-range values
            min_value = kwargs.get("min_value")
            max_value = kwargs.get("max_value")
            
            if min_value is not None:
                before_count = (clean_df[column] < min_value).sum()
                clean_df.loc[clean_df[column] < min_value, column] = min_value
                print(f"  Capped {before_count} values below minimum {min_value} in {column}")
            
            if max_value is not None:
                before_count = (clean_df[column] > max_value).sum()
                clean_df.loc[clean_df[column] > max_value, column] = max_value
                print(f"  Capped {before_count} values above maximum {max_value} in {column}")
        
        elif expectation_type == "expect_column_value_lengths_to_be_between":
            # Handle string length issues
            max_length = kwargs.get("max_value")
            if max_length is not None and clean_df[column].dtype == 'object':
                mask = clean_df[column].str.len() > max_length
                before_count = mask.sum()
                clean_df.loc[mask, column] = clean_df.loc[mask, column].str.slice(0, max_length)
                print(f"  Truncated {before_count} values in {column} to max length {max_length}")
    
    print(f"\nCleansing complete! Original shape: {df.shape}, Cleansed shape: {clean_df.shape}")
    
    return clean_df

def revalidate_cleansed_data(clean_df, suite):
    """
    Revalidate the cleansed data to confirm improvements.
    
    Args:
        clean_df: Cleansed DataFrame
        suite: ExpectationSuite used for initial validation
        
    Returns:
        tuple: (ge_df, validation_result)
    """
    print("\n" + "="*50)
    print("REVALIDATING CLEANSED DATA")
    print("="*50)
    
    # Convert to Great Expectations dataset
    ge_df = ge.from_pandas(clean_df)
    
    # Validate against the same suite
    validation_result = ge_df.validate(expectation_suite=suite, result_format="COMPLETE")
    
    # Print validation summary
    success = validation_result.success
    results_dict = validation_result.to_json_dict()
    
    print(f"\nRevalidation {'succeeded' if success else 'failed'}")
    print(f"Success rate: {results_dict['statistics']['success_percent']:.2f}%")
    print(f"Successful expectations: {results_dict['statistics']['successful_expectations']}")
    print(f"Evaluated expectations: {results_dict['statistics']['evaluated_expectations']}")
    
    # If there are still failures, print them
    if not success:
        print("\nRemaining Failed Expectations:")
        for result in results_dict["results"]:
            if not result["success"]:
                print(f"- {result['expectation_config']['expectation_type']} "
                      f"for {result['expectation_config']['kwargs'].get('column', 'N/A')}")
    
    return ge_df, validation_result

def generate_data_quality_report(original_df, cleansed_df, original_validation, cleansed_validation):
    """
    Generate a comprehensive data quality report.
    
    Args:
        original_df: Original DataFrame
        cleansed_df: Cleansed DataFrame
        original_validation: Validation result for original data
        cleansed_validation: Validation result for cleansed data
    """
    print("\n" + "="*50)
    print("DATA QUALITY REPORT")
    print("="*50)
    
    # Basic statistics
    print("\n1. BASIC STATISTICS")
    print("-" * 30)
    
    original_stats = original_df.describe().round(2)
    cleansed_stats = cleansed_df.describe().round(2)
    
    print("Original Data Statistics:")
    print(original_stats)
    
    print("\nCleansed Data Statistics:")
    print(cleansed_stats)
    
    # Missing values comparison
    print("\n2. MISSING VALUES COMPARISON")
    print("-" * 30)
    
    original_missing = original_df.isnull().sum()
    cleansed_missing = cleansed_df.isnull().sum()
    
    missing_comparison = pd.DataFrame({
        'Original Missing': original_missing,
        'Original Missing %': (original_missing / len(original_df) * 100).round(2),
        'Cleansed Missing': cleansed_missing,
        'Cleansed Missing %': (cleansed_missing / len(cleansed_df) * 100).round(2),
        'Improvement': original_missing - cleansed_missing
    })
    
    print(missing_comparison)
    
    # Validation results comparison
    print("\n3. VALIDATION RESULTS COMPARISON")
    print("-" * 30)
    
    original_results = original_validation.to_json_dict()
    cleansed_results = cleansed_validation.to_json_dict()
    
    print(f"Original Data Validation:")
    print(f"  Success: {original_results['success']}")
    print(f"  Success Rate: {original_results['statistics']['success_percent']:.2f}%")
    print(f"  Successful Expectations: {original_results['statistics']['successful_expectations']}")
    print(f"  Evaluated Expectations: {original_results['statistics']['evaluated_expectations']}")
    
    print(f"\nCleansed Data Validation:")
    print(f"  Success: {cleansed_results['success']}")
    print(f"  Success Rate: {cleansed_results['statistics']['success_percent']:.2f}%")
    print(f"  Successful Expectations: {cleansed_results['statistics']['successful_expectations']}")
    print(f"  Evaluated Expectations: {cleansed_results['statistics']['evaluated_expectations']}")
    
    improvement = (cleansed_results['statistics']['success_percent'] - 
                  original_results['statistics']['success_percent'])
    
    print(f"\nImprovement in Success Rate: {improvement:.2f}%")
    
    # Data shape comparison
    print("\n4. DATA SHAPE COMPARISON")
    print("-" * 30)
    
    print(f"Original Data Shape: {original_df.shape}")
    print(f"Cleansed Data Shape: {cleansed_df.shape}")
    print(f"Rows Removed: {len(original_df) - len(cleansed_df)}")
    print(f"Percentage of Data Retained: {(len(cleansed_df) / len(original_df) * 100):.2f}%")
    
    print("\n" + "="*50)
    print("END OF DATA QUALITY REPORT")
    print("="*50)

def main():
    """
    Main function to demonstrate Great Expectations with Airbyte data.
    """
    print("="*50)
    print("GREAT EXPECTATIONS DEMONSTRATION WITH AIRBYTE DATA")
    print("="*50)
    print("\nThis script demonstrates how to validate and cleanse data loaded from Airbyte using Great Expectations.")
    
    # Step 1: Load data from Airbyte
    df = load_data_from_airbyte(count=10_000, seed=123)  # Using a smaller count for faster execution
    
    # Step 2: Create an expectation suite
    suite = create_expectation_suite()
    
    # Step 3: Validate the data with Great Expectations
    ge_df, validation_result = validate_data_with_great_expectations(df, suite)
    
    # Step 4: Cleanse the data based on validation results
    cleansed_df = cleanse_data_based_on_validation(df, validation_result)
    
    # Step 5: Revalidate the cleansed data
    cleansed_ge_df, cleansed_validation = revalidate_cleansed_data(cleansed_df, suite)
    
    # Step 6: Generate a data quality report
    generate_data_quality_report(df, cleansed_df, validation_result, cleansed_validation)
    
    # Step 7: Demonstrate automatic profiling (optional)
    print("\n" + "="*50)
    print("BONUS: AUTOMATIC PROFILING DEMONSTRATION")
    print("="*50)
    
    profiled_ge_df, profiled_validation, profiled_suite = profile_and_validate_data(df)
    
    print("\n" + "="*50)
    print("GREAT EXPECTATIONS DEMONSTRATION COMPLETE")
    print("="*50)
    
    return cleansed_df

if __name__ == "__main__":
    # Execute the main function
    cleansed_df = main()
    
    # In a real application, you might save the cleansed data or pass it to the next step
    # For example:
    # cleansed_df.to_csv('cleansed_products.csv', index=False)
