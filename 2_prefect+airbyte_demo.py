import airbyte as ab
from prefect import flow, task
import pandas as pd
import numpy as np
from typing import Dict, Any

# First, define the tasks for our workflow

@task(name="Extract Data from Airbyte", 
      description="Extracts data from Airbyte source-faker and returns a products DataFrame",
      retries=3, 
      retry_delay_seconds=30)
def extract_data_from_airbyte(count: int = 50_000, seed: int = 123) -> pd.DataFrame:
    """
    Extract data from Airbyte source-faker and return a products DataFrame.
    
    Args:
        count: Number of records to generate
        seed: Random seed for reproducibility
        
    Returns:
        DataFrame containing products data
    """
    try:
        # Create and install the source
        source: ab.Source = ab.get_source("source-faker")
        
        # Configure the source
        source.set_config(
            config={
                "count": count,
                "seed": seed,
            },
        )
        
        # Verify the config and credentials
        check_result = source.check()
        print(f"Source check result: {check_result}")
        
        # Select all streams and read data
        source.select_all_streams()
        read_result: ab.ReadResult = source.read()
        
        # Transform to DataFrame
        products_df = read_result["products"].to_pandas()
        print(f"Successfully extracted {len(products_df)} products from Airbyte")
        
        return products_df
    
    except Exception as e:
        print(f"Error extracting data from Airbyte: {e}")
        raise

@task(name="Transform Products Data",
      description="Performs basic transformations on the products data",
      retries=2)
def transform_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the products DataFrame by cleaning and enriching the data.
    
    Args:
        products_df: Raw products DataFrame from Airbyte
        
    Returns:
        Transformed products DataFrame
    """
    try:
        # Make a copy to avoid modifying the original
        df = products_df.copy()
        
        # Basic cleaning
        # Convert price to numeric if it's not already
        if df['price'].dtype == 'object':
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            
        # Fill missing values
        df['price'].fillna(df['price'].mean(), inplace=True)
        
        # Add derived columns
        df['price_category'] = pd.cut(
            df['price'], 
            bins=[0, 50, 100, 500, float('inf')],
            labels=['Budget', 'Standard', 'Premium', 'Luxury']
        )
        
        # Add a discount column (random discount between 0-30%)
        np.random.seed(42)  # for reproducibility
        df['discount_pct'] = np.random.uniform(0, 0.3, size=len(df))
        df['discounted_price'] = df['price'] * (1 - df['discount_pct'])
        df['discounted_price'] = df['discounted_price'].round(2)
        
        print(f"Successfully transformed products data with {len(df)} rows")
        return df
    
    except Exception as e:
        print(f"Error transforming products data: {e}")
        raise

@task(name="Analyze Products Data",
      description="Performs analysis on the transformed products data")
def analyze_products_data(transformed_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze the transformed products data and return insights.
    
    Args:
        transformed_df: Transformed products DataFrame
        
    Returns:
        Dictionary containing analysis results
    """
    try:
        results = {}
        
        # Category distribution
        results['category_counts'] = transformed_df['category'].value_counts().to_dict()
        
        # Price statistics
        results['price_stats'] = {
            'mean': transformed_df['price'].mean(),
            'median': transformed_df['price'].median(),
            'min': transformed_df['price'].min(),
            'max': transformed_df['price'].max(),
            'std': transformed_df['price'].std()
        }
        
        # Price category distribution
        results['price_category_counts'] = transformed_df['price_category'].value_counts().to_dict()
        
        # Average discount by category
        results['avg_discount_by_category'] = transformed_df.groupby('category')['discount_pct'].mean().to_dict()
        
        # Total potential revenue
        results['total_revenue'] = {
            'original': transformed_df['price'].sum(),
            'after_discount': transformed_df['discounted_price'].sum(),
            'discount_savings': transformed_df['price'].sum() - transformed_df['discounted_price'].sum()
        }
        
        print("Successfully analyzed products data")
        return results
    
    except Exception as e:
        print(f"Error analyzing products data: {e}")
        raise

@task(name="Generate Report",
      description="Generates a report from the analysis results")
def generate_report(transformed_df: pd.DataFrame, analysis_results: Dict[str, Any]) -> None:
    """
    Generate a report from the analysis results.
    
    Args:
        transformed_df: Transformed products DataFrame
        analysis_results: Dictionary containing analysis results
    """
    try:
        print("\n" + "="*50)
        print("PRODUCTS DATA ANALYSIS REPORT")
        print("="*50)
        
        # Summary statistics
        print("\nDATASET SUMMARY:")
        print(f"Total products: {len(transformed_df)}")
        print(f"Unique categories: {len(analysis_results['category_counts'])}")
        print(f"Price range: ${analysis_results['price_stats']['min']:.2f} - ${analysis_results['price_stats']['max']:.2f}")
        print(f"Average price: ${analysis_results['price_stats']['mean']:.2f}")
        
        # Category breakdown
        print("\nCATEGORY BREAKDOWN:")
        for category, count in sorted(analysis_results['category_counts'].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {category}: {count} products")
        
        # Price category distribution
        print("\nPRICE CATEGORY DISTRIBUTION:")
        for category, count in analysis_results['price_category_counts'].items():
            percentage = (count / len(transformed_df)) * 100
            print(f"  {category}: {count} products ({percentage:.1f}%)")
        
        # Revenue analysis
        print("\nREVENUE ANALYSIS:")
        print(f"Total potential revenue: ${analysis_results['total_revenue']['original']:.2f}")
        print(f"Revenue after discounts: ${analysis_results['total_revenue']['after_discount']:.2f}")
        print(f"Total discount value: ${analysis_results['total_revenue']['discount_savings']:.2f}")
        print(f"Average discount: {(analysis_results['total_revenue']['discount_savings'] / analysis_results['total_revenue']['original'] * 100):.1f}%")
        
        print("\n" + "="*50)
        print("END OF REPORT")
        print("="*50 + "\n")
    
    except Exception as e:
        print(f"Error generating report: {e}")
        raise

# Define the main flow that orchestrates the tasks
@flow(name="Products Data Pipeline",
      description="End-to-end pipeline for extracting, transforming, and analyzing products data")
def products_pipeline(count: int = 50_000, seed: int = 123):
    """
    Main workflow that orchestrates the products data pipeline.
    
    Args:
        count: Number of records to generate in Airbyte
        seed: Random seed for reproducibility
    """
    # Extract data from Airbyte
    products_df = extract_data_from_airbyte(count=count, seed=seed)
    
    # Transform the data
    transformed_df = transform_products_data(products_df)
    
    # Analyze the transformed data
    analysis_results = analyze_products_data(transformed_df)
    
    # Generate a report
    generate_report(transformed_df, analysis_results)
    
    return transformed_df

# Run the pipeline if this script is executed directly
if __name__ == "__main__":
    print("Starting Products Data Pipeline with Prefect and Airbyte...")
    
    # Run the pipeline with default parameters
    result_df = products_pipeline()
    
    # Display a sample of the final DataFrame
    print("\nSample of processed data:")
    print(result_df.head())
    
    print("\nPipeline execution completed successfully!")
