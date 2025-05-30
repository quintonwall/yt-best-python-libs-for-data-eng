import airbyte as ab
import os
import logging
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_data_from_airbyte(count: int = 50_000, seed: int = 123) -> ab.ReadResult:
    """
    Extract data from Airbyte using the source-faker connector.
    
    Args:
        count: Number of records to generate
        seed: Random seed for reproducibility
        
    Returns:
        Airbyte ReadResult containing the extracted data
    """
    logger.info(f"Extracting data from Airbyte source-faker with {count} records")
    
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
    logger.info(f"Source check result: {check_result}")
    
    # Select all of the source's streams and read data into the internal cache
    source.select_all_streams()
    read_result: ab.ReadResult = source.read()
    
    # Get the products DataFrame
    products_df = read_result["products"].to_pandas()
    logger.info(f"Successfully extracted {len(products_df)} products from Airbyte")
    
    return read_result

def get_snowflake_credentials() -> Dict[str, str]:
    """
    Get Snowflake credentials from environment variables or prompt user.
    
    Returns:
        Dictionary containing Snowflake credentials
    """
    # Try to get credentials from environment variables
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    database = os.environ.get('SNOWFLAKE_DATABASE')
    schema = os.environ.get('SNOWFLAKE_SCHEMA')
    role = os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')  # Default to ACCOUNTADMIN if not specified
    
    # If any credentials are missing, prompt the user
    if not all([account, user, password, warehouse, database, schema]):
        logger.warning("Snowflake credentials not found in environment variables")
        print("Please enter your Snowflake credentials:")
        
        if not account:
            account = input("Snowflake Account (e.g., xy12345.us-east-1): ")
        if not user:
            user = input("Snowflake User: ")
        if not password:
            import getpass
            password = getpass.getpass("Snowflake Password: ")
        if not warehouse:
            warehouse = input("Snowflake Warehouse: ")
        if not database:
            database = input("Snowflake Database: ")
        if not schema:
            schema = input("Snowflake Schema: ")
        if not role:
            role = input("Snowflake Role (default: ACCOUNTADMIN): ") or "ACCOUNTADMIN"
    
    return {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "role": role
    }

def create_snowflake_destination(credentials: Dict[str, str]) -> ab.Destination:
    """
    Create and configure an Airbyte Snowflake destination.
    
    Args:
        credentials: Dictionary containing Snowflake credentials
        
    Returns:
        Configured Airbyte Snowflake destination
    """
    logger.info(f"Creating Airbyte Snowflake destination for account: {credentials['account']}")
    
    # Create and install the destination
    destination: ab.Destination = ab.get_destination("destination-snowflake")
    
    # Configure the destination
    destination.set_config(
        config={
            "host": f"{credentials['account']}.snowflakecomputing.com",
            "username": credentials["user"],
            "password": credentials["password"],
            "database": credentials["database"],
            "schema": credentials["schema"],
            "warehouse": credentials["warehouse"],
            "role": credentials["role"],
            "loading_method": {
                "method": "Internal Staging"
            }
        }
    )
    
    # Verify the config and credentials
    check_result = destination.check()
    logger.info(f"Destination check result: {check_result}")
    
    return destination

def write_to_snowflake_with_airbyte(
    read_result: ab.ReadResult,
    destination: ab.Destination,
    table_prefix: str = ""
) -> None:
    """
    Write data from an Airbyte ReadResult to Snowflake using an Airbyte destination.
    
    Args:
        read_result: Airbyte ReadResult containing the data to write
        destination: Configured Airbyte Snowflake destination
        table_prefix: Optional prefix for table names in Snowflake
    """
    logger.info("Writing data to Snowflake using Airbyte")
    
    # Get the available streams from the read result
    streams = read_result.streams
    logger.info(f"Available streams: {streams}")
    
    # Configure the destination to write all streams
    for stream in streams:
        destination.select_stream(stream)
    
    # Write the data to Snowflake
    write_result = destination.write(read_result)
    
    logger.info(f"Write result: {write_result}")
    logger.info("Successfully wrote data to Snowflake using Airbyte")

def write_to_snowflake_with_connector(
    read_result: ab.ReadResult,
    credentials: Dict[str, str],
    table_name: str = "PRODUCTS"
) -> None:
    """
    Alternative method: Write data from an Airbyte ReadResult to Snowflake using the Snowflake connector.
    
    Args:
        read_result: Airbyte ReadResult containing the data to write
        credentials: Dictionary containing Snowflake credentials
        table_name: Name of the table to write to in Snowflake
    """
    logger.info(f"Writing data to Snowflake table {table_name} using Snowflake connector")
    
    # Import necessary libraries
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    
    # Get the products DataFrame
    products_df = read_result["products"].to_pandas()
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=credentials["account"],
            user=credentials["user"],
            password=credentials["password"],
            warehouse=credentials["warehouse"],
            database=credentials["database"],
            schema=credentials["schema"],
            role=credentials["role"]
        )
        
        # Create table if it doesn't exist
        cursor = conn.cursor()
        
        # Get column names and types from DataFrame
        columns = []
        for col_name, dtype in zip(products_df.columns, products_df.dtypes):
            if "int" in str(dtype):
                col_type = "INTEGER"
            elif "float" in str(dtype):
                col_type = "FLOAT"
            elif "datetime" in str(dtype):
                col_type = "TIMESTAMP_NTZ"
            else:
                col_type = "VARCHAR(16777216)"  # Snowflake's maximum VARCHAR size
            
            columns.append(f'"{col_name}" {col_type}')
        
        # Create table SQL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info(f"Table {table_name} created or already exists")
        
        # Write DataFrame to Snowflake
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=products_df,
            table_name=table_name,
            quote_identifiers=True
        )
        
        if success:
            logger.info(f"Successfully wrote {num_rows} rows to Snowflake in {num_chunks} chunks")
        else:
            logger.error(f"Failed to write data to Snowflake: {output}")
        
        # Close the connection
        conn.close()
        logger.info("Snowflake connection closed")
    
    except Exception as e:
        logger.error(f"Error writing to Snowflake: {e}")
        raise

def main(
    count: int = 50_000,
    seed: int = 123,
    use_airbyte_destination: bool = True,
    table_name: str = "PRODUCTS",
    credentials: Optional[Dict[str, str]] = None
) -> None:
    """
    Main function to demonstrate extracting data from Airbyte and loading it to Snowflake.
    
    Args:
        count: Number of records to generate
        seed: Random seed for reproducibility
        use_airbyte_destination: Whether to use Airbyte's destination or the Snowflake connector
        table_name: Name of the table to write to in Snowflake (only used with Snowflake connector)
        credentials: Optional dictionary containing Snowflake credentials
    """
    print("="*50)
    print("AIRBYTE TO SNOWFLAKE DEMONSTRATION")
    print("="*50)
    
    try:
        # Step 1: Extract data from Airbyte
        read_result = extract_data_from_airbyte(count=count, seed=seed)
        
        # Display sample data
        products_df = read_result["products"].to_pandas()
        print("\nSample data extracted from Airbyte:")
        print(products_df.head())
        
        # Step 2: Get Snowflake credentials
        if credentials is None:
            credentials = get_snowflake_credentials()
        
        # Step 3: Write data to Snowflake
        if use_airbyte_destination:
            # Using Airbyte's Snowflake destination
            print("\nWriting data to Snowflake using Airbyte destination...")
            destination = create_snowflake_destination(credentials)
            write_to_snowflake_with_airbyte(read_result, destination)
        else:
            # Using Snowflake connector directly
            print("\nWriting data to Snowflake using Snowflake connector...")
            write_to_snowflake_with_connector(read_result, credentials, table_name)
        
        print("\nData successfully written to Snowflake!")
        
        print("\n" + "="*50)
        print("DEMONSTRATION COMPLETE")
        print("="*50)
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"\nError: {e}")
        print("\nPlease check your credentials and try again.")

if __name__ == "__main__":
    # Execute the main function
    # By default, use Airbyte's destination for writing to Snowflake
    main(count=10_000, use_airbyte_destination=True)
    
    # Alternatively, you can use the Snowflake connector directly:
    # main(count=10_000, use_airbyte_destination=False, table_name="AIRBYTE_PRODUCTS")
    
    # Note: In a real application, you would typically store Snowflake credentials
    # securely in environment variables or a configuration file, not hardcoded.
