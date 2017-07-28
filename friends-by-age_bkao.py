from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsCountByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
      fields = line.split(',')
      age = int(fields[2])
      numFriends = int(fields[3])
      return (age, numFriends)
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

#results = rdd.collect()
#for key, value in results:
#    print key, value
###    print("%s %i" % (key, value))  ### Same as the above statement

totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

#results = totalByAge.collect()

#for key, value in results:
#    print key, value

averagesByAge = totalByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect()
for result in results:
    print result
