from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("IncomingRevenueByCustomer")
sc = SparkContext(conf=conf)
def customerRevenuePair(raw):
    fields = raw.split(",")
    customerId = int(fields[0])
    revenue = float(fields[2])
    return (customerId, revenue)
rdd = sc.textFile('file:////Users/rwillians/Projects/Udemy/Spark/customer-orders.csv')
orders = rdd.map(customerRevenuePair)
totalRevenueByCustomer = orders.reduceByKey(lambda x, y: x + y) \
                               .map(lambda x: (x[1], x[0])) \
                               .sortByKey()
results = totalRevenueByCustomer.collect()
for result in results:
    print("{},{:.2f}".format(result[1], result[0]))
    
/*    
My partial output (ordered):

...
0,5524.95
41,5637.62
59,5642.89
42,5696.84
46,5963.11
97,5977.19
2,5994.59
71,5995.66
54,6065.39
39,6193.11
73,6206.20
68,6375.45
*/