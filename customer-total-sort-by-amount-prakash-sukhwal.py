from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Activity1")

sc = SparkContext(conf = conf)

def parseLine(line):

fields = line.split(',')

ID = int(fields[0])

amount = float(fields[2])

return (ID, amount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")

parsedLines = lines.map(parseLine)

unOrderedAmount = parsedLines.reduceByKey(lambda x, y: x+y)

orderedAmount = unOrderedAmount.map(lambda x:(x[1],x[0])).sortByKey()

results = orderedAmount.collect();

for result in results:

print(str(result[1]) + "\t{:.2f}".format(result[0]))

/*
Canopy 64bit) C:\SparkCourse>spark-submit activity2.py
45      3309.38
79      3790.57
96      3924.23
23      4042.65
99      4172.29
75      4178.50

...

43      5368.83
92      5379.28
6       5397.88
15      5413.51
63      5415.15
58      5437.73
32      5496.05
61      5497.48
85      5503.43
8       5517.24
0       5524.95
41      5637.62
59      5642.89
42      5696.84
46      5963.11
97      5977.19
2       5994.59
71      5995.66
54      6065.39
39      6193.11
73      6206.20
68      6375.45

*/