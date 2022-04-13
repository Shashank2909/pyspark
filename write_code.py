from pyspark.sql import SparkSession

spark = SparkSession.builder\
.master('local[4]')\
.appName("App")\
.config("spark.jars","/tmp/snowflake-jdbc-3.13.16.jar,/tmp/spark-snowflake_2.12-2.10.0-spark_3.2.jar").getOrCreate()

col = ['ID','Name','Age','Nationality','Overall','Club','Value','Preferred foot']
df = spark.read.csv('data.csv',header=True,inferSchema=True).select(col)

sfOptions = {
 "sfURL" : "https://ty57285.southeast-asia.azure.snowflakecomputing.com",
 "sfAccount" : "ty57285",
 "sfUser" : "shashank",
 "sfPassword" : "<password>",
 "sfDatabase" : "CITIBIKE",
 "sfSchema" : "public",
 "sfWarehouse" : "compute_wh",
 "sfRole" : "ACCOUNTADMIN",
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "players").mode("append").save()

