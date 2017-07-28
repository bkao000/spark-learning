import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def makePairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( (userID, ratings) ):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    print(movie1)
    print(rating1)
    print(movie2)
    print(rating2)
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
#
#print 5 records from the list
#results = ratings.take(5)
#results1 = ratings.collect()
#for result1 in results1:
#    print(result1)
#(196, (242, 3.0))
#(186, (302, 3.0))
#(22, (377, 1.0))
#(244, (51, 2.0))
#(166, (346, 1.0))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)
#
#print 5 records from the list
#results = joinedRatings.take(5)
results2 = joinedRatings.collect()
for result2 in results2:
    print(result2)
#(512, ((265, 4.0), (265, 4.0)))
#(512, ((265, 4.0), (23, 4.0)))
#(512, ((265, 4.0), (1, 4.0)))
#(512, ((265, 4.0), (198, 5.0)))
#(512, ((265, 4.0), (318, 5.0)))
#
#joinedRatings10 = joinedRatings.take(10)
#uniqueJoinedRatings = joinedRatings10.filter(filterDuplicates)
#AttributeError: 'list' object has no attribute 'filter'
#==> print does not work for 10 records

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
results3 = uniqueJoinedRatings.collect()
for result3 in results3:
    print(result3)

