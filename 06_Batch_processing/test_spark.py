import pyspark
from pyspark.sql import SparkSession
import time 

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()
print(f"ACTUAL Spark UI is running at: {spark.sparkContext.uiWebUrl}")
# This keeps the session open for 30 minutes so you can explore the UI
try:
    time.sleep(1800) 
finally:
    spark.stop() # Ensures session closes even if you Ctrl+C
