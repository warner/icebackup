
import os, sys, math, sqlite3
from abbreviate import abbreviate_space

db = sqlite3.connect(sys.argv[1])
c = db.cursor()
subpath = []
if len(sys.argv) > 2:
    subpath = sys.argv[2].split(os.sep)
rootpath, root_id = c.execute("SELECT rootpath, root_id FROM roots").fetchone()
node = root_id
for name in subpath:
    node = c.execute("SELECT id FROM nodes WHERE parentid=? AND name=?",
                     (node, name)).fetchone()[0]

items = []
for row in c.execute("SELECT name, cumulative_size, cumulative_items"
                     " FROM nodes WHERE parentid=?",
                     (node,)):
    items.append( (row[0], row[1], row[2]) )

maxname = max([len(name) for (name,size) in items])
fmt = "%" + "%d"%maxname + "s" + ": %d B (%s) [%d items]"
items.sort(key=lambda i: i[1])
for (name, size, cumulative_items) in items:
    print fmt % (name, size, abbreviate_space(size), cumulative_items)
