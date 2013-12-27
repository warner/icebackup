
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

row = c.execute("SELECT cumulative_size, cumulative_items FROM nodes WHERE id=?",
              (node,)).fetchone()
size = row[0]
print "%s: %d (%s), %d items" % (os.path.join(rootpath, *subpath),
                                 size, abbreviate_space(size),
                                 row[1])
