
import sys, os, hashlib
import dbutil
from abbreviate import abbreviate_space

schema = """
CREATE TABLE version -- added in v1
(
 version INTEGER  -- contains one row, set to 2
);

CREATE TABLE `nodes`
(
 `isdir` INTEGER, -- 0 or 1
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `parentid` INTEGER, -- or NULL for a root
 `name` VARCHAR,
 `size` INTEGER, -- bytes: length of file, or serialized directory
 `cumulative_size` INTEGER, -- sum of 'size' from this node and all children
 `cumulative_items` INTEGER,
 `filecap` VARCHAR
);

CREATE INDEX `parentid` ON `nodes` (`parentid`);

CREATE TABLE `roots`
(
 `rootpath` VARCHAR,
 `root_id` INTEGER
);

"""

class Scanner:
    def __init__(self, rootpath, dbfile):
        assert isinstance(rootpath, unicode)
        self.rootpath = os.path.abspath(rootpath)
        self.dbfile = dbfile
        self.db = dbutil.get_db(dbfile, create_version=(schema, 1),
                                synchronous="OFF")

    def process_directory(self, localpath, parentid):
        assert isinstance(localpath, unicode)
        # localpath is relative to self.rootpath
        abspath = os.path.join(self.rootpath, localpath)
        print "%sDIR: %s" % (" "*(len(localpath.split(os.sep))-1), localpath)
        s = os.stat(abspath)
        size = s.st_size # good enough for now
        name = os.path.basename(os.path.abspath(abspath))
        dirid = self.db.execute("INSERT INTO nodes"
                                " (parentid, name, isdir, size)"
                                " VALUES (?,?,?,?)", (parentid, name, 1, size)
                                ).lastrowid
        cumulative_size = size
        cumulative_items = 1
        for child in os.listdir(abspath):
            childpath = os.path.join(localpath, child)

            if os.path.isdir(os.path.join(self.rootpath, childpath)):
                new_dirid, subtree_size, subtree_items = \
                           self.process_directory(childpath, dirid)
                cumulative_size += subtree_size
                cumulative_items += subtree_items
            elif os.path.islink(os.path.join(self.rootpath, childpath)):
                pass
            else:
                new_fileid, file_size = self.process_file(childpath, dirid)
                cumulative_size += file_size
                cumulative_items += 1

        self.db.execute("UPDATE nodes"
                        " SET cumulative_size=?, cumulative_items=?"
                        " WHERE id=?",
                        (cumulative_size, cumulative_items, dirid))
        return dirid, cumulative_size, cumulative_items

    def process_file(self, localpath, parentid):
        assert isinstance(localpath, unicode)
        abspath = os.path.join(self.rootpath, localpath)
        name = os.path.basename(os.path.abspath(abspath))
        print "%sFILE %s" % (" "*(len(localpath.split(os.sep))-1), name)

        s = os.stat(abspath)
        size = s.st_size
        fileid = self.db.execute("INSERT INTO nodes "
                                 "(parentid, name, isdir,"
                                 " size, cumulative_size, cumulative_items)"
                                 " VALUES (?,?,?,?,?,?)",
                                 (parentid, name, 0, size, size, 1)
                                 ).lastrowid
        filecap = self.upload_file(abspath)
        self.db.execute("UPDATE nodes SET filecap=? WHERE id=?",
                        (filecap, fileid))
        return fileid, size

    def upload_file(self, abspath):
        return "fake filecap"
        f = open(abspath, "rb")
        h = hashlib.sha256()
        while True:
            data = f.read(32*1024)
            if not data:
                break
            h.update(data)
        f.close()
        filecap = "file:%s" % h.hexdigest()
        return filecap

def main():
    dbname = sys.argv[1]
    assert dbname.endswith(".sqlite"), dbname
    root = sys.argv[2].decode("utf-8")
    assert os.path.isdir(root), root
    s = Scanner(root, dbname)
    rootid, cumulative_size, cumulative_items = s.process_directory(u".", None)
    s.db.execute("INSERT INTO roots (rootpath, root_id) VALUES (?,?)",
                 (root, rootid))
    s.db.commit()
    print "rootid #%s" % (rootid,)
    print "cumulative_size %d (%s)" % (cumulative_size,
                                       abbreviate_space(cumulative_size))
    print "cumulative_items %d" % cumulative_items

if __name__ == "__main__":
    main()
