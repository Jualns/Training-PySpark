from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from StartSparkSession import start_spark

# Initialize Spark session
spark = start_spark()
file_path = "../Training-PySpark/datasets/customers.csv"

def SQL_solution(spark: SparkSession):
    query = """
        SELECT customer_id, name, purchase_amount 
        FROM customers 
        WHERE purchase_amount >= 100 AND age >= 30;
    """

    # Using SQL to solve the problem
    selected_df = spark.sql(query)

    return selected_df

def PySpark_solution(df: DataFrame):
    "Solution based on pyspark without SQL"
    # Apply filter
    df_filtred = df.filter((df.purchase_amount >= 100) & (df.age >= 30))
    # Apply Select
    df_selected = df_filtred.select(col("customer_id"), col("name"), col("purchase_amount"))

    return df_selected


try:
    # Read the file
    df = spark.read.format('csv').option('header', 'true').load(file_path)

    # Registering the DataFrame as a temporary view
    df.createOrReplaceTempView("customers")

    selected_df = SQL_solution(spark)

    # Display the final DataFrame
    selected_df.show()

    selected_df_py = PySpark_solution(df)

    # Display the final DataFrame
    selected_df_py.show()
except Exception as err:
    print(f"Erro: {err}")
finally:
    spark.stop()