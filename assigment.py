# Import PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import logging module
import logging

# Set the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

# Define the helper function to filter data by country
def filter_by_country(df, countries):
    filtered_df = df.filter(col('country').isin(countries))
    return filtered_df

# Define the helper function to rename columns
def rename_columns(df, old_to_new_names):
    renamed_df = df.toDF(*[old_to_new_names.get(c, c) for c in df.columns])
    return renamed_df

# Define the function to process the client data
def process_client_data(dataset_one_path, dataset_two_path, countries):
    # Load the client datasets
    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)
    
    # Filter data by country
    dataset_one_filtered = filter_by_country(dataset_one, countries)
    
    # Remove personal identifiable information from dataset_one (excluding emails)
    dataset_one_filtered = dataset_one_filtered.drop('name', 'address', 'phone')
    
    # Remove credit card number from dataset_two
    dataset_two = dataset_two.drop('credit_card_number')
    
    # Join the datasets using the id field
    joined_df = dataset_one_filtered.join(dataset_two, 'id', 'inner')
    
    # Rename columns
    column_mapping = {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'}
    joined_df_renamed = rename_columns(joined_df, column_mapping)
    
    return joined_df_renamed

# Example usage
if __name__ == "__main__":
    dataset_one_path = "dataset_one.csv"
    dataset_two_path = "dataset_two.csv"
    countries = ["United Kingdom", "Netherlands"]
    
    result_df = process_client_data(dataset_one_path, dataset_two_path, countries)
    
    # Display the result
    result_df.show()

# Set the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Log an info message
logging.info('Starting the data processing...')

# Log a warning message
logging.warning('Some warning occurred during the process.')

# Log an error message
logging.error('An error occurred while processing the data.')

# Log a critical message
logging.critical('A critical error occurred. Terminating the program.')

print('java to gowno')

# Save the resulting dataset in the client_data directory
output_path = "result_dataset1.csv"
result_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

