from pyspark.sql import SparkSession, DataFrame # type: ignore
import os

def load_koppen_model(spark: SparkSession, data: DataFrame):
    data.write \
        .format("jdbc") \
        .option(
            "url",
            f"jdbc:postgresql://{os.getenv('DWH_DB_HOST')}:{os.getenv('DWH_DB_PORT')}/{os.getenv('DWH_DB_NAME')}",
        ) \
        .option("dbtable", "silver.koppen_model") \
        .option("user", os.getenv("DWH_DB_USER")) \
        .option("password", os.getenv("DWH_DB_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .save()
            

