from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("new_app").master("local[*]").\
config("spark.jars","file:///home/shashank/Practice/mysql-connector-java-5.1.46.jar").getOrCreate()

mysql_db_driver_class = "com.mysql.jdbc.Driver"
table_name = "emp"
host_name = "localhost"
port_name = str(3306)
user_name = "shashank"
password = "shashank"
db_name = "hr"

mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_name + "/" + db_name
query = "select * from emp"
sqlOptions = {
	"url":mysql_jdbc_url,
	"driver": "com.mysql.jdbc.Driver",
	"query":query,
	"user":"shashank",
	"password":"shashank"
}


df = spark.read.format("jdbc").options(**sqlOptions).load()

df.show(10)



