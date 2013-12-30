
import sys, math, sqlite3
from abbreviate import abbreviate_space

db = sqlite3.connect(sys.argv[1])
c = db.cursor()
buckets = [ (0,0), (1,3)]
root = math.sqrt(10)

def next_power_of_k(n, k):
    if n == 0:
        x = 0
    else:
        x = int(math.log(n, k) + 0.5)
    if k**x < n:
        return k**(x+1)
    else:
        return k**x

def which_bucket(size):
    # return (min,max) such that min <= size <= max
    # values are from the set (0,0), (1,3), (4,10), (11,31), (32,100),
    # (101,316), (317, 1000), etc: two per decade
    assert size >= 0
    i = 0
    while True:
        if i >= len(buckets):
            # extend the list
            new_lower = buckets[i-1][1]+1
            new_upper = int(next_power_of_k(new_lower, root))
            buckets.append( (new_lower, new_upper) )
        maybe = buckets[i]
        if maybe[0] <= size <= maybe[1]:
            return maybe
        i += 1

size_histogram = {}
c.execute("SELECT size FROM nodes WHERE isdir=0")
for row in c.fetchall():
    bucket = which_bucket(row[0])
    if bucket not in size_histogram:
        size_histogram[bucket] = 0
    size_histogram[bucket] += 1

for bucket in buckets:
    #s = "%9s-%s" % (bucket[0], bucket[1])
    s = "(%s)-(%s)" % (abbreviate_space(bucket[0]), abbreviate_space(bucket[1]))
    print "%24s: %8d" % (s, size_histogram.get(bucket, 0))

c.execute("SELECT COUNT(*) FROM nodes WHERE isdir=0")
print "%d total files" % (c.fetchone()[0])
c.execute("SELECT SUM(size) FROM nodes WHERE isdir=0")
print "%s total file bytes" % abbreviate_space(c.fetchone()[0])
c.execute("SELECT COUNT(*) FROM nodes WHERE isdir=1")
print "%d directories" % (c.fetchone()[0])
