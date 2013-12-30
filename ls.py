
import os, sys, math, sqlite3
from abbreviate import abbreviate_space

db = sqlite3.connect(sys.argv[1])
db.row_factory = sqlite3.Row
c = db.cursor()
subpath = []
if len(sys.argv) > 2:
    subpath = sys.argv[2].split(os.sep)

row = c.execute("SELECT * FROM snapshots WHERE finished IS NOT NULL"
                " ORDER BY finished DESC LIMIT 1").fetchone()
snapshotid = row["id"]
rootpath = row["rootpath"]
root_id = row["root_id"]

node = root_id
for name in subpath:
    node = c.execute("SELECT id FROM dirtable WHERE parentid=? AND name=?",
                     (node, name)).fetchone()[0]

items = []
for row in c.execute("SELECT name, cumulative_size, cumulative_items"
                     " FROM dirtable WHERE parentid=?",
                     (node,)):
    items.append( (row["name"],row["cumulative_size"],row["cumulative_items"]) )
for row in c.execute("SELECT name, size"
                     " FROM filetable WHERE parentid=?",
                     (node,)):
    items.append( (row["name"],row["size"],1) )

maxname = max([len(row[0]) for row in items])
fmt = "%" + "%d"%maxname + "s" + ": %d B (%s) [%d items]"
items.sort(key=lambda i: i[1]) # sort by size, largest last
#items.sort(key=lambda i: i[1]) # sore by name
for (name, size, cumulative_items) in items:
    print fmt % (name, size, abbreviate_space(size), cumulative_items)
