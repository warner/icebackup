
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
    node = c.execute("SELECT id FROM dirtable WHERE parentid=? AND name=?",
                     (node, name)).fetchone()[0]

row = c.execute("SELECT * FROM dirtable WHERE id=?", (node,)).fetchone()
size = row["cumulative_size"]
print "%s: %d (%s), %d items" % (os.path.join(rootpath, *subpath),
                                 size, abbreviate_space(size),
                                 row["cumulative_items"])
