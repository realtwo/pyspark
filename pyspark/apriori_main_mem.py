# Each line is one transaction
# Items in each line (transaction) are seperated by space

import operator

support = 100

def convert_to_int_index():
    # convert string to integer
    fp = open('browsing.txt', 'r')
    fp_out = open('browsing_int', 'w')
    map_str_to_int = {}  # holds item name: int id mapping
    map_int_to_str = {}  # holds int id: item name mapping

    id_cnt = 0
    for line in fp:
        line_out = ''
        for v in line.split():
            if v not in map_str_to_int:
                map_str_to_int[v] = id_cnt
                map_int_to_str[id_cnt] = v
                id_cnt+=1
            line_out = line_out + str(map_str_to_int[v]) + ' '
        fp_out.write(line_out+'\n')

    fp_out.close()
    fp.close()
    print 'Total number of items: ' + str(id_cnt)
    return map_int_to_str


map_int_to_str = convert_to_int_index()
id_cnt = len(map_int_to_str)

# now the apriori
fp = open('browsing_int', 'r')
one_count = [0]*id_cnt

# first pass
for line in fp:
    for v in line.split():
        one_count[int(v)]+=1

fp.close()


# second pass
fp = open('browsing_int', 'r')
two_count = {}
for line in fp:
    items = [int(v) for v in line.split()]
    items = sorted(items)

    for i in xrange(0, len(items)):
        if one_count[items[i]]<support:
            continue
        for j in xrange(i+1, len(items)):
            if one_count[items[j]] < support:
                continue
            if (items[i], items[j]) in two_count:
                two_count[(items[i], items[j])]+=1
            else:
                two_count[(items[i], items[j])]=1

fp.close()
#print 'Two counts:'
#print len(two_count)

# drop infrequent items:
freq_two = []
freq_two = {k:v for (k,v) in two_count.iteritems() if v>=support}

#print 'Frequent twos:'
#print freq_two
#print len(freq_two)

# cal confidence
confidence_two={}
for (k,v) in freq_two.iteritems():
    confidence_two[k] = float(v)/one_count[k[0]]
    confidence_two[(k[1], k[0])]= float(v)/one_count[k[1]]

sorted_conf = sorted(confidence_two.items(), key=operator.itemgetter(1), reverse=True)

for (k,v) in sorted_conf[0:15]:
    print (map_int_to_str[k[0]], map_int_to_str[k[1]],v)