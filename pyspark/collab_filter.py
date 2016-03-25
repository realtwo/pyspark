# Input: matrix
# elements are space separated
# row: ratings for n items by a user
# column: rating for an item by m users
# entry for (i,j) is binary: 0 or 1


from pyspark import SparkConf, SparkContext
from math import sqrt

user_id = 2

conf = SparkConf().setMaster("local").setAppName("APrioriAlgo")
sc = SparkContext(conf = conf)

inputRdd = sc.textFile("data_user_item.txt")
matRdd = inputRdd.map(lambda x: [int(v) for v in x.split()])

m = matRdd.count()
oneRow = matRdd.first()
n = len(oneRow)

print "Total number of rows (users): " + str(m)
print "Total number of cols (items): " + str(n)

idxRdd = sc.parallelize(range(0, matRdd.count()))

rowMatRdd = idxRdd.zip(matRdd)  #(row index, row of matrix)

rowMatRdd.persist()

def getRow(rowMatRdd, idx):
	return rowMatRdd.filter(lambda x: x[0]==idx).map(lambda x: x[1])

def calNorm(row):
	return sqrt(sum(row))

userRow = getRow(rowMatRdd, user_id-1).first()
userNorm = calNorm(userRow)

#TODO: this takes a long time...
userSim = []
for i in xrange(0, m):
	row = getRow(rowMatRdd, i).first()
	rowNorm = calNorm(row)
	s = 0
	for j in xrange(0, n):
		s += row[j]*userRow[j]
	userSim.append(s/userNorm/rowNorm)

print userSim



