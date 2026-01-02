from pyspark.sql import SparkSession, DataFrame # type: ignore
import os

def read_weather_hist(spark: SparkSession, query: str) -> DataFrame:
    data = spark.read \
        .format("jdbc") \
        .option(
            "url",
            f"jdbc:postgresql://{os.getenv('DWH_DB_HOST')}:{os.getenv('DWH_DB_PORT')}/{os.getenv('DWH_DB_NAME')}",
        ) \
        .option("query", query) \
        .option("user", os.getenv("DWH_DB_USER")) \
        .option("password", os.getenv("DWH_DB_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    return data