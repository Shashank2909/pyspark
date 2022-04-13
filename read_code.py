from pyspark.sql import SparkSession

spark = SparkSession.builder\
.master('local[4]')\
.appName("App")\
.config("spark.jars","/tmp/snowflake-jdbc-3.13.16.jar,/tmp/spark-snowflake_2.12-2.10.0-spark_3.2.jar").getOrCreate()


sfOptions = {
 "sfURL" : "https://jv04488.southeast-asia.azure.snowflakecomputing.com",
 "sfAccount" : "jv04488",
 "sfUser" : "snow",
 "sfPassword" : "<password>",
 "sfDatabase" : "snowflake_sample_data",
 "sfSchema" : "TPCH_SF1",
 "sfWarehouse" : "compute_wh",
 "sfRole" : "ACCOUNTADMIN",
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sdf = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions,header=False).option("dbtable", "customer").load()

spark.sql("create database db3")
spark.sql("use db3;")
sdf.write.saveAsTable("custTable1",format='csv')

sfOptions = {
 "sfURL" : "https://jv04488.southeast-asia.azure.snowflakecomputing.com",
 "sfAccount" : "jv04488",
 "sfUser" : "snow",
 "sfPassword" : "<password>",
 "sfDatabase" : "snowflake_sample_data",
 "sfSchema" : "TPCH_SF10",
 "sfWarehouse" : "compute_wh",
 "sfRole" : "ACCOUNTADMIN",
}

sdf = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions,header=True).option("dbtable", "customer").load()


sdf.write.saveAsTable("custTable2",format='csv')
