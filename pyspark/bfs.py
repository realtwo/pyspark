from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("BFS")
sc = SparkContext(conf = conf)

hitCounter = sc.accumulator(0)

src_id = 1
dst_id = 6

def parseInput(line):
	l = line.split()
	v_id = int(l[0])
	
	if v_id == src_id:
		v_dist = 0
		v_status = 1
	else:
		v_dist = 9999
		v_status = 0

	v_conn = [int(v) for v in l[1:]]
	v_data = (v_conn, v_dist, v_status)
	return (v_id, v_data)


def bfsMap(node):
	v_id = node[0]
	v_data = node[1]
	v_conn = v_data[0]
	v_dist = v_data[1]
	v_status = v_data[2]

	ret = []

	if v_status == 1:
		for conn in v_conn:
			newId = conn
			newDist = v_dist +1
			newStatus = 1
			if v_id == dst_id:
				print "Found: " + str(v_id)
				hitCounter.add(1)
			node = (newId, ([], newDist, newStatus))
			ret.append(node)

		v_status = 2
	ret.append((v_id, (v_conn, v_dist, v_status)))

	return ret


def bfsReduce(data1, data2):
	#print "Reducing"
	#print data1
	#print data2
	conn1 = data1[0]
	conn2 = data2[0]
	dist1 = data1[1]
	dist2 = data2[1]
	status1 = data1[2]
	status2 = data2[2]

	conn = conn1 + conn2
	dist = min(dist1, dist2)	
	status = max(status1, status2)

	#print (conn, dist, status)
	return(conn, dist, status)


def main():
	print "Running bfs..."
	graphRdd = sc.textFile("graph.txt").map(parseInput)

	for i in xrange(0, 10):
		print '---------------------------'
		dist_cnt=i+1
		print dist_cnt

		tgtRdd = graphRdd.flatMap(bfsMap)

		print "Searching %d connections" %(tgtRdd.count())

		if hitCounter.value > 0:
			print "Found target in %d steps from %d directions" %(dist_cnt, hitCounter.value)
			break
	
		graphRdd = tgtRdd.reduceByKey(bfsReduce)
		print graphRdd.collect()

	return dist_cnt


if __name__ == '__main__':
	main()
