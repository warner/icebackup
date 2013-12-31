
import os, sys, math, sqlite3, collections
from abbreviate import abbreviate_space, abbreviate_time

db = sqlite3.connect(sys.argv[1])
db.row_factory = sqlite3.Row
c = db.cursor()

subpath = []
if len(sys.argv) > 2:
    subpath = sys.argv[2].split(os.sep)

MB = 1000*1000
bucket_edges = [ (0,1*MB-1) ]
for i in range(1, 10):
    bucket_edges.append( (i*MB, (i+1)*MB-1) )
for i in range(10, 100, 10):
    bucket_edges.append( (i*MB, (i+10)*MB-1) )
bucket_edges.append( (100*MB, 100*MB) )
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
        if i >= len(bucket_edges):
            # extend the list
            new_lower = bucket_edges[i-1][1]+1
            new_upper = int(next_power_of_k(new_lower, root))
            bucket_edges.append( (new_lower, new_upper) )
        maybe = bucket_edges[i]
        if maybe[0] <= size <= maybe[1]:
            return maybe
        i += 1

class Bucket:
    count = 0
    raw_size = 0
    padded_size = 0
buckets = collections.defaultdict(Bucket)

sizes = []
for row in db.execute("SELECT * FROM upload_schedule"):
    upid = row["id"]
    upsize = db.execute("SELECT SUM(size) FROM upload_schedule_files"
                        " WHERE upload_schedule_id=?",
                        (upid,)).fetchone()[0]
    sizes.append(upsize)
    edges = which_bucket(upsize)
    bucket = buckets[edges]
    bucket.count += 1
    bucket.raw_size += upsize
    bucket.padded_size += upsize + 32768

template = "{0:^24}: {1:>9} {2:>9}  {3:>9}  {4:>9}"
print template.format("bucket edges", "count", "raw_size", "w/pad", "pad")
print template.format("------------", "-----", "--------", "-----", "---")
for edges in bucket_edges:
    bucket = buckets[edges]
    edges_s = "{0:>11}-{1:<11}".format("(%s)" % abbreviate_space(edges[0]),
                                       "(%s)" % abbreviate_space(edges[1]))
    if bucket.raw_size == 0:
        overhead_perc = 0
    else:
        overhead_perc = 100.0*(bucket.padded_size - bucket.raw_size)/bucket.raw_size
    pad = bucket.padded_size - bucket.raw_size
    print template.format(edges_s,
                          bucket.count,
                          abbreviate_space(bucket.raw_size),
                          abbreviate_space(bucket.padded_size),
                          #"%1.1f%%" % overhead_perc,
                          abbreviate_space(pad),
                          )
total_raw_size = sum([b.raw_size for b in buckets.values()])
total_count = sum([b.count for b in buckets.values()])
total_pad = 32768 * total_count
total_padded = total_raw_size + total_pad
print template.format("==", "=", "=", "=", "=")
print template.format("total", total_count, abbreviate_space(total_raw_size),
                      abbreviate_space(total_padded),
                      abbreviate_space(total_pad))
