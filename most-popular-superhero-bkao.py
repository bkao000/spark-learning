from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostPopularSuperhero")
sc = SparkContext(conf = conf)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8")) 
    
def countCoOccurrences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) -1)    

names = sc.textFile("file:///sparkcourse/marvel-names.txt")
namesRdd = names.map(parseNames)

#results = namesRdd.collect()
#for result in results:
#    print(result)

lines = sc.textFile("file:///sparkcourse/marvel-graph.txt")
pairings = lines.map(countCoOccurrences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x,y : x+y)

#results = totalFriendsByCharacter.collect()
#for result in results:
#    print(result)

flipped = totalFriendsByCharacter.map(lambda (x,y) : (y,x))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print mostPopularName + " is the most popular superhero, with " + str(mostPopular[0]) + " co-appearances."

