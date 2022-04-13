from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").config("spark.jars","/tmp/mysql-connector-java-5.1.46.jar").getOrCreate()

df = spark.read.csv("just_csv/part-00000-711688c5-26be-4ff4-848f-09e8a736a460-c000.csv",header=True,inferSchema=True)

df.printSchema()


mysql_db_driver_class = "com.mysql.jdbc.Driver"
table_name = "players"
host_name = "localhost"
port_name = str(3306)
user_name = "shashank"
password = "shashank"
db_name = "hr"

mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_name + "/" + db_name
query = ""
sqlOptions = {
	"url":mysql_jdbc_url,
	"driver": "com.mysql.jdbc.Driver",
	"dbtable": "hr.players",
	"user":"shashank",
	"password":"shashank"
}

df.write.format("jdbc").mode("overwrite").options(**sqlOptions).save()



