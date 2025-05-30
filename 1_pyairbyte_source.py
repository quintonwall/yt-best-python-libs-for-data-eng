import airbyte as ab

# Create and install the source:
source: ab.Source = ab.get_source("source-faker")
# Configure the source
source.set_config(
    config={
        "count": 50_000,  # Adjust this to get a larger or smaller dataset
        "seed": 123,
    },
)
# Verify the config and creds by running `check`:
source.check()
# Select all of the source's streams and read data into the internal cache:
source.select_all_streams()
read_result: ab.ReadResult = source.read()
# Display or transform the loaded data
products_df = read_result["products"].to_pandas()
display(products_df)

