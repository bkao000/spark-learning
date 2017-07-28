from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Get sum of each customer's purchase amount").getOrCreate()
df = spark.read.csv("c:/SparkCourse/customer-orders.csv")
df.createOrReplaceTempView("orders")

df.show()

ordersumDF = spark.sql("SELECT  _c0 as ID,sum(float(_c2))  as total_amount FROM orders  group by _c0 order by 2")
ordersumDF.show()



#Some of the results:

#+---+------------------+
#| ID|      total_amount|
#+---+------------------+
#| 45|3309.3800055980682|
#| 79| 3790.569982469082|
#| 96|3924.2299877405167|
#| 23| 4042.650001913309|
#| 99| 4172.290024012327|
#| 75| 4178.499995291233|