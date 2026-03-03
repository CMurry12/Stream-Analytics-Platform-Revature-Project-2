#!/usr/bin/env python3

from pyspark.sql import SparkSession

import os

os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# spark = SparkSession.builder \
#     .appName("Local PySpark") \
#     .master("local[*]") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("First Pyspark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Test your Spark session
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
df.show()

data = [("John", 28), ("Jane", 25)]
columns = ["Name", "Age"]


df2 = spark.createDataFrame(data, columns)
df2.show()
df3=df2.coalesce(1)

#df3.write.format("csv").mode("overwrite").save("file:///opt/spark/work-dir/output")
df3.write.format("csv").mode("append").save("file:///opt/spark-data")