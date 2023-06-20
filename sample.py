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

def process_client_data(client_data_path, financial_data_path, countries):
    # Load client datasets
    client_data = spark.read.csv(client_data_path, header=True)
    financial_data = spark.read.csv(financial_data_path, header=True)

    # Filter data by countries
    filtered_client_data = client_data.filter(col("country").isin(countries))

    # Remove personal identifiable information
    filtered_client_data = filtered_client_data.drop("name", "address")

    # Remove credit card number
    filtered_financial_data = financial_data.drop("cc_number")

    # Join datasets using id field
    joined_data = filtered_client_data.join(filtered_financial_data, on="id")

    # Rename columns
    renamed_data = joined_data.withColumnRenamed("id", "client_identifier") \
                              .withColumnRenamed("btc_a", "bitcoin_address") \
                              .withColumnRenamed("cc_t", "credit_card_type")

    return renamed_data


# Example usage
client_data_path = "dataset_one.csv"
financial_data_path = "dataset_two.csv"
countries = ["United Kingdom", "Netherlands"]

result = process_client_data(client_data_path, financial_data_path, countries)
result.show()

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