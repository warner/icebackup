
import os, sys, math, sqlite3
from abbreviate import abbreviate_space

db = sqlite3.connect(sys.argv[1])
db.row_factory = sqlite3.Row
c = db.cursor()
subpath = []
if len(sys.argv) > 2:
    subpath = sys.argv[2].split(os.sep)

row = c.execute("SELECT * FROM snapshots WHERE finished IS NOT NULL"
                " ORDER BY finished ASC LIMIT 1").fetchone()
snapshotid = row["id"]
rootpath = row["rootpath"]
root_id = row["root_id"]

node = root_id
for name in subpath:
    node = c.execute("SELECT id FROM nodes WHERE"
                     " snapshotid=? AND parentid=? AND name=?",
                     (snapshotid, node, name)).fetchone()[0]

items = []
for row in c.execute("SELECT name, cumulative_size, cumulative_items"
                     " FROM nodes WHERE parentid=?",
                     (node,)):
    items.append( (row[0], row[1], row[2]) )

maxname = max([len(row[0]) for row in items])
fmt = "%" + "%d"%maxname + "s" + ": %d B (%s) [%d items]"
items.sort(key=lambda i: i[1])
for (name, size, cumulative_items) in items:
    print fmt % (name, size, abbreviate_space(size), cumulative_items)
