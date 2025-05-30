import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from datetime import datetime
import os
from typing import Dict, Any, Optional, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_sample_dataframe(num_records: int = 100) -> pd.DataFrame:
    """
    Create a sample pandas DataFrame with random data.
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        DataFrame with sample data
    """
    logger.info(f"Creating sample DataFrame with {num_records} records")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Generate sample data
    data = {
        'id': range(1, num_records + 1),
        'name': [f"Product {i}" for i in range(1, num_records + 1)],
        'category': np.random.choice(
            ['Electronics', 'Clothing', 'Home', 'Books', 'Sports'],
            size=num_records
        ),
        'price': np.round(np.random.uniform(10, 1000, size=num_records), 2),
        'stock': np.random.randint(0, 100, size=num_records),
        'rating': np.round(np.random.uniform(1, 5, size=num_records), 1),
        'created_at': [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
            for _ in range(num_records)
        ]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    logger.info(f"Sample DataFrame created with shape: {df.shape}")
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    return df

def connect_to_snowflake(
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str
) -> snowflake.connector.SnowflakeConnection:
    """
    Connect to Snowflake using the provided credentials.
    
    Args:
        account: Snowflake account identifier
        user: Snowflake username
        password: Snowflake password
        warehouse: Snowflake warehouse name
        database: Snowflake database name
        schema: Snowflake schema name
        
    Returns:
        Snowflake connection object
    """
    logger.info(f"Connecting to Snowflake account: {account}")
    
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

def create_snowflake_table(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str
) -> None:
    """
    Create a table in Snowflake if it doesn't exist.
    
    Args:
        conn: Snowflake connection object
        table_name: Name of the table to create
    """
    logger.info(f"Creating Snowflake table: {table_name} if it doesn't exist")
    
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER,
            name VARCHAR(255),
            category VARCHAR(50),
            price FLOAT,
            stock INTEGER,
            rating FLOAT,
            created_at TIMESTAMP_NTZ
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info(f"Table {table_name} created or already exists")
        
        cursor.close()
    
    except Exception as e:
        logger.error(f"Error creating Snowflake table: {e}")
        raise

def write_dataframe_to_snowflake(
    conn: snowflake.connector.SnowflakeConnection,
    df: pd.DataFrame,
    table_name: str
) -> Tuple[bool, Dict[str, Any]]:
    """
    Write a pandas DataFrame to a Snowflake table.
    
    Args:
        conn: Snowflake connection object
        df: DataFrame to write
        table_name: Name of the target Snowflake table
        
    Returns:
        Tuple containing success flag and result details
    """
    logger.info(f"Writing DataFrame with {len(df)} rows to Snowflake table: {table_name}")
    
    try:
        # Ensure the table exists
        create_snowflake_table(conn, table_name)
        
        # Write the DataFrame to Snowflake
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            quote_identifiers=False
        )
        
        result = {
            "success": success,
            "num_chunks": num_chunks,
            "num_rows": num_rows,
            "output": output
        }
        
        if success:
            logger.info(f"Successfully wrote {num_rows} rows to Snowflake in {num_chunks} chunks")
        else:
            logger.error(f"Failed to write data to Snowflake: {output}")
        
        return success, result
    
    except Exception as e:
        logger.error(f"Error writing DataFrame to Snowflake: {e}")
        raise

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
    
    return {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema
    }

def query_snowflake_table(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    limit: int = 10
) -> pd.DataFrame:
    """
    Query data from a Snowflake table and return as a DataFrame.
    
    Args:
        conn: Snowflake connection object
        table_name: Name of the Snowflake table to query
        limit: Maximum number of rows to return
        
    Returns:
        DataFrame containing query results
    """
    logger.info(f"Querying up to {limit} rows from Snowflake table: {table_name}")
    
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        result_df = pd.read_sql(query, conn)
        
        logger.info(f"Retrieved {len(result_df)} rows from Snowflake")
        return result_df
    
    except Exception as e:
        logger.error(f"Error querying Snowflake table: {e}")
        raise

def main(
    table_name: str = "SAMPLE_PRODUCTS",
    num_records: int = 100,
    credentials: Optional[Dict[str, str]] = None
) -> None:
    """
    Main function to demonstrate writing pandas DataFrame to Snowflake.
    
    Args:
        table_name: Name of the target Snowflake table
        num_records: Number of sample records to generate
        credentials: Optional dictionary containing Snowflake credentials
    """
    print("="*50)
    print("PANDAS TO SNOWFLAKE DEMONSTRATION")
    print("="*50)
    
    try:
        # Step 1: Create a sample DataFrame
        df = create_sample_dataframe(num_records)
        
        # Display sample data
        print("\nSample DataFrame:")
        print(df.head())
        
        # Step 2: Get Snowflake credentials
        if credentials is None:
            credentials = get_snowflake_credentials()
        
        # Step 3: Connect to Snowflake
        conn = connect_to_snowflake(
            account=credentials["account"],
            user=credentials["user"],
            password=credentials["password"],
            warehouse=credentials["warehouse"],
            database=credentials["database"],
            schema=credentials["schema"]
        )
        
        # Step 4: Write DataFrame to Snowflake
        success, result = write_dataframe_to_snowflake(conn, df, table_name)
        
        if success:
            # Step 5: Query the data back from Snowflake
            print("\nVerifying data in Snowflake:")
            result_df = query_snowflake_table(conn, table_name)
            print(result_df.head())
            
            print(f"\nSuccessfully wrote {result['num_rows']} rows to Snowflake table: {table_name}")
        else:
            print(f"\nFailed to write data to Snowflake: {result['output']}")
        
        # Close the connection
        conn.close()
        logger.info("Snowflake connection closed")
        
        print("\n" + "="*50)
        print("DEMONSTRATION COMPLETE")
        print("="*50)
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        print(f"\nError: {e}")
        print("\nPlease check your Snowflake credentials and try again.")

if __name__ == "__main__":
    # Execute the main function
    main()
    
    # Note: In a real application, you would typically store Snowflake credentials
    # securely in environment variables or a configuration file, not hardcoded.
    # 
    # Example with hardcoded credentials (NOT RECOMMENDED for production):
    # 
    # credentials = {
    #     "account": "your_account_id.region",
    #     "user": "your_username",
    #     "password": "your_password",
    #     "warehouse": "your_warehouse",
    #     "database": "your_database",
    #     "schema": "your_schema"
    # }
    # main(credentials=credentials)
