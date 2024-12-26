# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

try:

    #enter the file path here
    file_path = "../Training-PySpark/datasets/customers.csv"
    

    #read the file
    df = spark.read.format('csv').option('header', 'true').load(file_path)

    # Registering the DataFrame as a temporary view
    df.createOrReplaceTempView("customers")

    query = """
    SELECT customer_id, name, purchase_amount 
    FROM customers 
    WHERE purchase_amount >= 100 AND age >= 30;
    """

    # Using SQL to select columns
    selected_df = spark.sql(query)

    # Display the final DataFrame using the display() function.
    selected_df.show()

    # Solution based on pyspark without SQL
    selected_df_py = df.filter((df.purchase_amount >= 100) & (df.age >= 30)) \
                .select(col("customer_id"), col("name"), col("purchase_amount"))

    selected_df_py.show()

except Exception as err:
    print(f"Erro: {err}")
finally:
    spark.stop()