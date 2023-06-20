from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import logging module
import logging

# Set the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder.appName('KommatiPara').getOrCreate()