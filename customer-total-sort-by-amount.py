from pyspark import SparkConf, SparkContext

def customer_amount(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2])) 

conf = SparkConf().setMaster("local").setAppName("CustomerTotal")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///sparkcourse/customer-orders.csv")

orderAmt = lines.map(customer_amount).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()
results = orderAmt.collect()
for result in results:
    print("%d: %f" % (result[1], result[0]))
