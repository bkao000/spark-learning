from pyspark import SparkConf, SparkContext
 
conf= SparkConf().setMaster("local").setAppName("CustomerSpent")
sc=SparkContext(conf=conf )
 
lines=sc.textFile("/Users/anmoljain/Desktop/CMU/Sem4/Spark/examples/customer-orders.csv")
 
field=lines.map(lambda x:x.split(","))
 
customerSpent=field.map(lambda x:(x[0],float(x[2])))
 
custSpentTotal=customerSpent.reduceByKey(lambda x,y:x+y)
 
finalCustSpent= custSpentTotal.collect()
 
for cust,spent in finalCustSpent:
    print "%s: %0.2f"%(cust,spent)
 
 
/* 
Output:
 
4: 5259.92
25: 5057.61
26: 5250.40
27: 4915.89
20: 4836.86
21: 4707.41
22: 5019.45
23: 4042.65
28: 5000.71
*/