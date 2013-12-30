
import sys, math, sqlite3, collections
from abbreviate import abbreviate_space, abbreviate_time

db = sqlite3.connect(sys.argv[1])
c = db.cursor()
bucket_edges = [ (0,0), (1,3)]
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
snapshotid = c.execute("SELECT id FROM snapshots WHERE finished IS NOT NULL"
                       " ORDER BY finished ASC LIMIT 1").fetchone()[0]
c.execute("SELECT size FROM filetable WHERE snapshotid=?",
          (snapshotid,))
for row in c.fetchall():
    size = row[0]
    edges = which_bucket(size)
    bucket = buckets[edges]
    bucket.count += 1
    bucket.raw_size += size
    bucket.padded_size += size + 32768


template = "{0:^23}: {1:>9} {2:>9}  {3:>9}  {4:>9}"
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

c.execute("SELECT COUNT(*) FROM dirtable WHERE snapshotid=?", (snapshotid,))
print "%d directories" % (c.fetchone()[0])

def money(val):
    return "$%1.2f" % val
REQ_COST = 0.05/1000
UPLOAD_RATE = 260e3 # 2Mbps = 260kBps
STORAGE_COST = 0.01/1e9
DOWNLOAD_RATE = 3.6e6 # 28Mbps = 3.6MBps
RETRIEVAL_COST_AT_MAX = 7.20 * (DOWNLOAD_RATE / (1e9/3600))
TRANSFER_COST = 0.12/1e9

print "upload: req=%s, time=%s" % (money(REQ_COST*total_count),
                                   abbreviate_time(total_raw_size / UPLOAD_RATE))
print "storage: %s/mo" % money(STORAGE_COST*total_padded)
down_req = REQ_COST*total_count
down_time = total_raw_size / DOWNLOAD_RATE
down_months = math.ceil(down_time / (30*24*3600))
retrieval_cost = down_months * RETRIEVAL_COST_AT_MAX
xfer_cost = TRANSFER_COST * total_raw_size
down_cost = down_req + retrieval_cost + xfer_cost

print "download (@13GB/hr): %s (req=%s + retr=%s + xfer=%s), time=%s" % (
    money(down_cost), money(down_req), money(retrieval_cost), money(xfer_cost),
    abbreviate_time(down_time))

down_time = total_raw_size / (4e9/3600)
down_months = math.ceil(down_time / (30*24*3600))
retrieval_cost = down_months * 7.2*4
down_cost = down_req + retrieval_cost + xfer_cost

print "download (@4GB/hr) : %s (req=%s + retr=%s + xfer=%s), time=%s" % (
    money(down_cost), money(down_req), money(retrieval_cost), money(xfer_cost),
    abbreviate_time(down_time))

down_time = total_raw_size / (1e9/3600)
down_months = math.ceil(down_time / (30*24*3600))
retrieval_cost = down_months * 7.2*1
down_cost = down_req + retrieval_cost + xfer_cost

print "download (@1GB/hr) : %s (req=%s + retr=%s + xfer=%s), time=%s" % (
    money(down_cost), money(down_req), money(retrieval_cost), money(xfer_cost),
    abbreviate_time(down_time))

def buckets_in_range(start, end):
    for edge in bucket_edges:
        (edge_start,edge_end) = edge
        if (edge_start >= start and
            edge_end <= end):
            yield buckets[edge]

for name,start,end in [("<1MB",1,1e6),
                       ("1MB-100MB", 1e6,100e6),
                       (">100MB", 100e6, 10e9)]:
    count, raw_size = 0,0
    for bucket in buckets_in_range(start, end):
        count += bucket.count
        raw_size += bucket.raw_size
    print "{0:>10} {1:>9} {2}".format(name, count, abbreviate_space(raw_size))

