
import sys, os
import dbutil

schema = """
CREATE TABLE version -- added in v1
(
 version INTEGER  -- contains one row, set to 2
);

CREATE TABLE `nodes`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `parentid` INTEGER, -- or NULL for a root
 `isdir` INTEGER, -- 0 or 1
 `size` INTEGER, -- bytes: length of file, or serialized directory
 `cumulative_size` INTEGER, -- sum of 'size' from this node and all children
 `filecap` VARCHAR
);

"""

class Scanner:
    def __init__(self, rootpath, dbfile):
        self.rootpath = os.path.abspath(rootpath)
        self.dbfile = dbfile
        self.db = dbutil.get_db(dbfile, create_version=(schema, 1),
                                synchronous="OFF")

    def process_directory(self, localpath, parentid):
        # localpath is relative to self.rootpath
        abspath = os.path.join(self.rootpath, localpath)
        print "DIR", abspath
        s = os.stat(abspath)
        size = s.st_size # good enough for now
        dirid = self.db.execute("INSERT INTO nodes (parentid, isdir, size)"
                                " VALUES (?,1,?)", (parentid, size)
                                ).lastrowid
        cumulative_size = size
        for child in os.listdir(abspath):
            childpath = os.path.join(localpath, child)

            if os.path.isdir(os.path.join(self.rootpath, childpath)):
                new_dirid, subtree_size = self.process_directory(childpath,
                                                                 dirid)
                cumulative_size += subtree_size
            else:
                new_fileid, file_size = self.process_file(childpath, dirid)
                cumulative_size += file_size

        self.db.execute("UPDATE nodes SET cumulative_size=? WHERE id=?",
                        (cumulative_size, dirid))
        return dirid, cumulative_size

    def process_file(self, localpath, parentid):
        abspath = os.path.join(self.rootpath, localpath)
        print "FILE", abspath
        s = os.stat(abspath)
        size = s.st_size
        fileid = self.db.execute("INSERT INTO nodes (parentid, isdir, size)"
                                 " VALUES (?,0,?)", (parentid, size)
                                 ).lastrowid
        return fileid, size

def main():
    s = Scanner(sys.argv[1], "icebackup.sqlite")
    rootid, cumulative_size = s.process_directory(".", None)
    s.db.commit()
    print "rootid", rootid
    print "cumulative_size", cumulative_size

if __name__ == "__main__":
    main()
