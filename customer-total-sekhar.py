from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Get sum of each customer's purchase amount").getOrCreate()
df = spark.read.csv("c:/SparkCourse/customer-orders.csv")
df.createOrReplaceTempView("orders")

df.show()

ordersumDF = spark.sql("SELECT  _c0 as ID,sum(float(_c2))  as total_amount FROM orders  group by _c0")


ordersumDF.show()

#Some of the results:

#+---+------------------+
#| ID|      total_amount|
#+---+------------------+
#| 51| 4975.219970226288|
#|  7| 4755.070008277893|
#| 15| 5413.510010659695|
#| 54| 6065.390002984554|