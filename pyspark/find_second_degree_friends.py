#input data format:
# id0	 id01, id02 ...
# id1	 id11, id12 ...
#...
# first id (idn) is current node
# following ids (idnm) are nodes connected to idn.

# second degree: two nodes not directly connected
# but through a third node that is connected to both.


from pyspark import SparkConf, SparkContext
import itertools


conf = SparkConf().setMaster("local").setAppName("FirstDegreeFriend")
sc = SparkContext(conf = conf)

inputRdd = sc.textFile("data_friend.txt")

def genSecondDegreeFriend(line):
    v = line.split()
    
    if len(v)<2: #lonely without friends
    	return []
    
    result = []
    friends = [int(elem) for elem in v[1].split(',')] 
    for f in itertools.product(friends, friends):
    	if f[0] != f[1]:  # 
    		result.append((f, 1))
    for f in friends:
    	key = (int(v[0]), f)  #direct friends
    	result.append((key, 0))
    return result

def recommendFriend(l):
	sortedFriends = sorted(l, key=lambda x: x[1], reverse=True)
	recommFriends = [v[0] for v in sortedFriends]

	if len(recommFriends) > 10:  #only take top 10
		return recommFriends[0:10]

	return recommFriends


print "Generate friend pairs"
friendRdd = inputRdd.flatMap(genSecondDegreeFriend)  


print "Generate (friend_id, count)"
friendRdd = friendRdd.reduceByKey(lambda x,y: x*y*(x+y)) \
    					  .filter(lambda x: x[1]>0) \
    					  .map(lambda x: (x[0][0],(x[0][1], x[1]))) 

print "Preparing for recommendation"
friendRdd = friendRdd.groupByKey().mapValues(list)  # this is most time consuming
print friendRdd.take(5)

print "Generate recommendation"
recommendRdd = friendRdd.mapValues(recommendFriend)
print recommendRdd.take(5)




