import pyspark
import pandas as pd
from scipy.stats import skew, kurtosis
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType

# Creating the Spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName('Python Spark SQL Basic Example') \
    .config('spark.jars', 'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\PostgreSQL 16\postgresql-42.2.18.jar') \
    .getOrCreate()

def movies_to_df():
    # Read the data from the table using Spark JDBC
    movies_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/etl_pipeline') \
        .option('dbtable', 'movies') \
        .option('user', 'postgres') \
        .option('password', 'root') \
        .option('driver', 'org.postgresql.Driver') \
        .load()
    return movies_df

# Define pandas UDFs for skewness and kurtosis
@pandas_udf(DoubleType())
def calculate_skewness(column: pd.Series) -> float:
    return float(skew(column.dropna()))

@pandas_udf(DoubleType())
def calculate_kurtosis(column: pd.Series) -> float:
    return float(kurtosis(column.dropna()))

def summarize_data(df):
    # Compute descriptive statistics for numerical features
    numeric_stats = df.describe().toPandas()
    
    # Compute skewness and kurtosis for numerical columns
    numerical_columns = [c for c in df.columns if df.schema[c].dataType.typeName() in ['double', 'float']]
    skewness = {}
    kurtosis_values = {}

    for column in numerical_columns:
        try:
            skewness[column] = df.select(calculate_skewness(col(column))).collect()[0][0]
        except Exception as e:
            print(f"Error calculating skewness for {column}: {e}")
            skewness[column] = None
        
        try:
            kurtosis_values[column] = df.select(calculate_kurtosis(col(column))).collect()[0][0]
        except Exception as e:
            print(f"Error calculating kurtosis for {column}: {e}")
            kurtosis_values[column] = None
    
    # Combine the stats into a single DataFrame
    combined_stats = {
        'numeric': numeric_stats,
        'skewness': pd.DataFrame(list(skewness.items()), columns=['Column', 'Skewness']),
        'kurtosis': pd.DataFrame(list(kurtosis_values.items()), columns=['Column', 'Kurtosis'])
    }
    
    return combined_stats

def load_df_to_db(df_dict):
    mode = 'overwrite'
    url = 'jdbc:postgresql://localhost:5432/etl_pipeline'
    properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }
    
    for key, df in df_dict.items():
        if isinstance(df, pd.DataFrame):  # Convert pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(df)
            spark_df.write.jdbc(
                url=url,
                table=f'summary_stats_{key}',
                mode=mode,
                properties=properties
            )

if __name__ == "__main__":
    movies_df = movies_to_df()
    summary_df = summarize_data(movies_df)
    load_df_to_db(summary_df)
