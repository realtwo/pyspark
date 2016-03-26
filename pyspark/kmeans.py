# Input: 
# data_kmeans: each line is one point, with features space separated
# data_centroid_init: initial centroids

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Kmeans")
sc = SparkContext(conf = conf)

inputRdd = sc.textFile("data_kmeans.txt") \
				.map(lambda x: (-1, [float(v) for v in x.split()]))  #(cluster id, coordinate)
initCentroidRdd = sc.textFile("data_centroid_init.txt") 

iter_max = 20
n_cluster= initCentroidRdd.count()
print 'Clustering into %d clustes' %(n_cluster)

idxRdd = sc.parallelize(range(0, n_cluster))

centroidRdd = idxRdd.zip(initCentroidRdd) \
					.map(lambda x: (x[0], [float(v) for v in x[1].split()]))

centroidMap =  centroidRdd.collectAsMap()

print centroidMap


def calDist(l1, l2):
	if len(l1)!=len(l2):
		print "Error: dimension mismatch"
		return -1  # TODO: raise exception

	s = 0
	for i in xrange(0, len(l1)):
		s += (l1[i]-l2[i])**2
	return s

def sumList(l1, l2):
	return [v[0]+v[1] for v in zip(l1, l2)]

def divList(l, n):
	return [v/n for v in l]

def assignCluster(v):
	ptData = v[1]

	cluster_idx =-1
	min_dist = -1
	for i in xrange(0, n_cluster):
		ptCentroid = centroidMap[i]
		dist = calDist(ptCentroid, ptData)
		if min_dist < 0 or dist < min_dist:
			min_dist = dist 
			cluster_idx = i

	return (cluster_idx, ptData)



ptRdd = inputRdd
for iter_count in xrange(0, iter_max):
	# assign points to cluster
	ptRdd = inputRdd.map(assignCluster)

	# recompute centroid
	centroidMap = ptRdd.map(lambda x: (x[0], (x[1], 1))) \
						.reduceByKey(lambda x, y: (sumList(x[0], y[0]), x[1]+y[1])) \
						.map(lambda x: (x[0], (divList(x[1][0], x[1][1])))) \
						.collectAsMap()

	if len(centroidMap)<n_cluster:
		print 'Bad initial centroids. Reassign.'
		break

	print 'Centroids: '
	print centroidMap



