import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\vinch\PycharmProjects\data_engineering\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\vinch\PycharmProjects\data_engineering\spark-env\Scripts\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("testo_py") \
    .master("local[*]") \
    .getOrCreate()

# Exemple de code Spark
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["Name", "Value"])
df.show()

spark.stop()
