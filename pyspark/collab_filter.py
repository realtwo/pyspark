# Input: matrix
# elements are space separated
# row: ratings for n items by a user
# column: rating for an item by m users
# entry for (i,j) is binary: 0 or 1


from pyspark import SparkConf, SparkContext
from math import sqrt

user_id = 2
item_limit = 2

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

def getCol(rowMatRdd, idx): 
	return rowMatRdd.map(lambda x: x[1][idx])

def calNorm(row):
	return sqrt(sum(row))

#item-item based collaborate filter
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

estRating =[]  # user-user
for idx_col in xrange(0, item_limit):
	col = getCol(rowMatRdd, idx_col).collect()
	print col
	s = 0
	for j in xrange(0, m):
		s+=userSim[j]*col[j]
	estRating.append(s)

print "predicted rating based on user-user collab filter: "
print estRating

#item-item based collaborate filter
estRating2 = []  #item-item 
for idx_col in xrange(0, item_limit):
	itemCol = getCol(rowMatRdd, idx_col).collect()
	itemNorm = calNorm(itemCol)
	itemSim =[]
	for j in xrange(0, n):
		col = getCol(rowMatRdd, j).collect()
		colNorm = calNorm(col)
		s = 0
		for k in xrange(0, m):
			s+=itemCol[k]*col[k]
		itemSim.append(s/itemNorm/colNorm)
	print itemSim

	s = 0
	for j in xrange(0, n):
		s+=itemSim[j]*userRow[j]
	estRating2.append(s)

print "predicted rating based on item-item collab filter: "
print estRating2


