import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import airbyte as ab

# This script demonstrates how to cleanse data that has been loaded from Airbyte

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
            "count": count,  # Adjust this to get a larger or smaller dataset
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

def explore_data(df):
    """
    Explore the data to identify issues that need cleansing.
    """
    print("\n" + "="*50)
    print("DATA EXPLORATION")
    print("="*50)
    
    # Basic info
    print("\nDataFrame Info:")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Data types
    print("\nData Types:")
    print(df.dtypes)
    
    # Missing values
    print("\nMissing Values:")
    missing = df.isnull().sum()
    missing_percent = (missing / len(df)) * 100
    missing_info = pd.DataFrame({
        'Missing Values': missing,
        'Percentage': missing_percent
    })
    print(missing_info[missing_info['Missing Values'] > 0])
    
    # Duplicates
    print(f"\nDuplicate Rows: {df.duplicated().sum()}")
    
    # Basic statistics
    print("\nBasic Statistics:")
    print(df.describe())
    
    # Sample data
    print("\nSample Data:")
    print(df.head())
    
    return df

def cleanse_data(df):
    """
    Cleanse the data by addressing common issues.
    """
    print("\n" + "="*50)
    print("DATA CLEANSING")
    print("="*50)
    
    # Make a copy to avoid modifying the original
    clean_df = df.copy()
    
    # Step 1: Handle missing values
    print("\nStep 1: Handling missing values...")
    
    # Check missing values before cleansing
    missing_before = clean_df.isnull().sum()
    
    # For product_id and name: drop rows with missing values (these are critical fields)
    clean_df = clean_df.dropna(subset=['product_id', 'name'])
    print(f"Dropped {len(df) - len(clean_df)} rows with missing product_id or name")
    
    # For price: fill with median (mean could be affected by outliers)
    clean_df['price'] = clean_df['price'].fillna(clean_df['price'].median())
    
    # For stock: fill with 0 (assuming missing stock means out of stock)
    clean_df['stock'] = clean_df['stock'].fillna(0)
    
    # For rating: fill with median
    clean_df['rating'] = clean_df['rating'].fillna(clean_df['rating'].median())
    
    # For description: fill with empty string
    clean_df['description'] = clean_df['description'].fillna('')
    
    # For created_at: fill with current date
    clean_df['created_at'] = clean_df['created_at'].fillna(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Check missing values after cleansing
    missing_after = clean_df.isnull().sum()
    print("Missing values before and after cleansing:")
    print(pd.DataFrame({
        'Before': missing_before,
        'After': missing_after
    }))
    
    # Step 2: Handle duplicates
    print("\nStep 2: Handling duplicates...")
    
    # Check for duplicate product_ids
    duplicate_ids = clean_df['product_id'].duplicated()
    print(f"Found {duplicate_ids.sum()} duplicate product_ids")
    
    # Keep first occurrence of each product_id
    clean_df = clean_df.drop_duplicates(subset=['product_id'], keep='first')
    print(f"After removing duplicates: {len(clean_df)} rows")
    
    # Step 3: Standardize formats
    print("\nStep 3: Standardizing formats...")
    
    # Standardize product_id (uppercase, remove empty strings)
    clean_df['product_id'] = clean_df['product_id'].str.upper()
    clean_df = clean_df[clean_df['product_id'].str.strip() != '']
    
    # Standardize name (title case, strip whitespace)
    clean_df['name'] = clean_df['name'].str.strip().str.title()
    
    # Standardize category (title case)
    clean_df['category'] = clean_df['category'].str.title()
    
    # Step 4: Handle outliers
    print("\nStep 4: Handling outliers...")
    
    # For price: cap at 3 standard deviations from mean
    price_mean = clean_df['price'].mean()
    price_std = clean_df['price'].std()
    price_upper_limit = price_mean + 3 * price_std
    
    outliers_price = clean_df[clean_df['price'] > price_upper_limit]
    print(f"Found {len(outliers_price)} price outliers > ${price_upper_limit:.2f}")
    
    # Cap the outliers
    clean_df.loc[clean_df['price'] > price_upper_limit, 'price'] = price_upper_limit
    
    # For rating: cap between 1 and 5
    outliers_rating = clean_df[(clean_df['rating'] < 1) | (clean_df['rating'] > 5)]
    print(f"Found {len(outliers_rating)} rating outliers outside range [1, 5]")
    
    clean_df['rating'] = clean_df['rating'].clip(1, 5)
    
    # Step 5: Convert data types
    print("\nStep 5: Converting data types...")
    
    # Convert price to float with 2 decimal places
    clean_df['price'] = clean_df['price'].round(2)
    
    # Convert stock to integer
    clean_df['stock'] = clean_df['stock'].astype(int)
    
    # Convert rating to float with 1 decimal place
    clean_df['rating'] = clean_df['rating'].round(1)
    
    # Try to convert created_at to datetime
    try:
        # First attempt with standard format
        clean_df['created_at'] = pd.to_datetime(clean_df['created_at'], errors='coerce')
        
        # For any that failed, try with different formats
        mask = clean_df['created_at'].isna()
        if mask.any():
            for format_str in ['%Y/%m/%d', '%d-%m-%Y', '%m/%d/%Y']:
                still_na = clean_df['created_at'].isna()
                clean_df.loc[still_na, 'created_at'] = pd.to_datetime(
                    clean_df.loc[still_na, 'created_at'], 
                    format=format_str, 
                    errors='coerce'
                )
        
        # Fill any remaining NaT with current date
        clean_df['created_at'] = clean_df['created_at'].fillna(pd.Timestamp.now())
        
        print(f"Converted created_at to datetime. Invalid dates: {mask.sum()}")
    except Exception as e:
        print(f"Error converting dates: {e}")
        # Keep as string if conversion fails
        pass
    
    # Step 6: Add derived columns
    print("\nStep 6: Adding derived columns...")
    
    # Add price category
    clean_df['price_category'] = pd.cut(
        clean_df['price'],
        bins=[0, 50, 200, 500, float('inf')],
        labels=['Budget', 'Standard', 'Premium', 'Luxury']
    )
    
    # Add stock status
    clean_df['stock_status'] = pd.cut(
        clean_df['stock'],
        bins=[-1, 0, 10, 50, float('inf')],
        labels=['Out of Stock', 'Low Stock', 'Medium Stock', 'High Stock']
    )
    
    # Add rating category
    clean_df['rating_category'] = pd.cut(
        clean_df['rating'],
        bins=[0, 2, 3.5, 5],
        labels=['Poor', 'Average', 'Good']
    )
    
    # Add description length
    clean_df['description_length'] = clean_df['description'].str.len()
    
    # Step 7: Final validation
    print("\nStep 7: Final validation...")
    
    # Ensure no missing values in critical columns
    critical_cols = ['product_id', 'name', 'price', 'stock', 'rating']
    missing_critical = clean_df[critical_cols].isnull().sum().sum()
    print(f"Missing values in critical columns: {missing_critical}")
    
    # Ensure all numeric columns have valid ranges
    print(f"Products with negative prices: {(clean_df['price'] < 0).sum()}")
    print(f"Products with negative stock: {(clean_df['stock'] < 0).sum()}")
    print(f"Products with ratings outside [1,5]: {((clean_df['rating'] < 1) | (clean_df['rating'] > 5)).sum()}")
    
    print(f"\nCleansing complete! Original shape: {df.shape}, Cleansed shape: {clean_df.shape}")
    
    return clean_df

def analyze_cleansed_data(clean_df):
    """
    Analyze the cleansed data to demonstrate the improvements.
    """
    print("\n" + "="*50)
    print("CLEANSED DATA ANALYSIS")
    print("="*50)
    
    # Basic statistics after cleansing
    print("\nBasic Statistics After Cleansing:")
    print(clean_df.describe())
    
    # Distribution of categorical columns
    print("\nCategory Distribution:")
    print(clean_df['category'].value_counts())
    
    print("\nPrice Category Distribution:")
    print(clean_df['price_category'].value_counts())
    
    print("\nStock Status Distribution:")
    print(clean_df['stock_status'].value_counts())
    
    print("\nRating Category Distribution:")
    print(clean_df['rating_category'].value_counts())
    
    # Sample of cleansed data
    print("\nSample of Cleansed Data:")
    print(clean_df.head())
    
    return clean_df

def main():
    """
    Main function to demonstrate the data cleansing process.
    """
    print("="*50)
    print("PANDAS DATA CLEANSING DEMONSTRATION")
    print("="*50)
    print("\nThis script demonstrates how to cleanse data loaded from Airbyte.")
    
    # Load data from Airbyte
    df = load_data_from_airbyte(count=10_000, seed=123)  # Using a smaller count for faster execution
    
    # Explore the data
    explore_data(df)
    
    # Cleanse the data
    clean_df = cleanse_data(df)
    
    # Analyze the cleansed data
    analyze_cleansed_data(clean_df)
    
    print("\n" + "="*50)
    print("DATA CLEANSING PROCESS COMPLETE")
    print("="*50)
    
    return clean_df

if __name__ == "__main__":
    # Execute the main function
    cleansed_df = main()
    
    # In a real application, you might save the cleansed data or pass it to the next step
    # For example:
    # cleansed_df.to_csv('cleansed_products.csv', index=False)
    # or
    # next_step_in_pipeline(cleansed_df)
