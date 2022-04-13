from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("newApp")\
.config("spark.jars","/tmp/snowflake-jdbc-3.13.16.jar,/tmp/spark-snowflake_2.12-2.10.0-spark_3.2.jar").getOrCreate()

sfOptions = {
 "sfURL" : "https://jv04488.southeast-asia.azure.snowflakecomputing.com",
 "sfAccount" : "jv04488",
 "sfUser" : "snow",
 "sfPassword" : "<password>",
 "sfDatabase" : "CITIBIKE",
 "sfSchema" : "public",
 "sfWarehouse" : "compute_wh",
 "sfRole" : "ACCOUNTADMIN",
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.csv("just_csv/part-00000-711688c5-26be-4ff4-848f-09e8a736a460-c000.csv",inferSchema=True,header=True)

df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "players").mode("append").options(header=True).save()
