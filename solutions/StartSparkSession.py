from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def start_spark(name: str = 'TrainingSpark') -> SparkSession:
    "Return a generic TrainingSpark Session"
    spark = SparkSession.builder.appName(name).getOrCreate()
    return spark

if __name__ == "__main__":
    spark = start_spark()

    spark.stop()