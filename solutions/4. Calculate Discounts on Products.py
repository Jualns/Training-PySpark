from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from StartSparkSession import start_spark
from pyspark.sql import functions

def SQL_solution(spark: SparkSession):
    query = """
    SELECT 
        product_id, 
        product_name, 
        original_price * (1 - discount_percentage / 100) AS final_price
    FROM
        products
    """

    # Using SQL to solve the problem
    selected_df = spark.sql(query)

    return selected_df

def PySpark_solution(df: DataFrame):
    "Solution based on pyspark without SQL"
    result_df = df.withColumn(
        "final_price",
        col("original_price") * (1 - col("discount_percentage") / 100)
    )

    return result_df.select(col("product_id"), col("product_name"), col("final_price"))

# Initialize Spark session
spark = start_spark()
file_path = "../Training-PySpark/datasets/products.csv"

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