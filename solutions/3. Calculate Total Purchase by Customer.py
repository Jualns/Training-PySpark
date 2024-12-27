from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from StartSparkSession import start_spark
from pyspark.sql import functions

def SQL_solution(spark: SparkSession):
    query = """
        SELECT customer_id, CAST(SUM(purchase_amount) AS INTEGER) AS total_purchase 
        FROM customers_purchase 
        GROUP BY customer_id
        ORDER BY customer_id
        """

    # Using SQL to solve the problem
    selected_df = spark.sql(query)

    return selected_df

def PySpark_solution(df: DataFrame):
    "Solution based on pyspark without SQL"
    # Apply filter
    df_selected = df.groupBy("customer_id").agg(functions.sum("purchase_amount").cast("int").alias("total_purchase")).orderBy("customer_id")

    return df_selected

# Initialize Spark session
spark = start_spark()
file_path = "../Training-PySpark/datasets/customers_purchase.csv"

try:
    #read the file
    df = spark.read.format('csv').option('header', 'true') \
                                    .option("mode", "DROPMALFORMED") \
                                    .load(file_path)

    # Registering the DataFrame as a temporary view
    df.createOrReplaceTempView(file_path.split("/")[-1].split(".")[0])

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