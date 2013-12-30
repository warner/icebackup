
import sys, os, hashlib, time
import dbutil
from abbreviate import abbreviate_space

schema = """
CREATE TABLE version -- added in v1
(
 version INTEGER  -- contains one row, set to 2
);

CREATE TABLE `snapshots`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `started` INTEGER,
 `finished` INTEGER,
 `rootpath` VARCHAR,
 `root_id` INTEGER
);

CREATE TABLE `nodes`
(
 `snapshotid` INTEGER,
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

CREATE TABLE `dirtable`
(
 `snapshotid` INTEGER,
 `path` VARCHAR,
 `metadata_json` VARCHAR
);

CREATE TABLE `filetable`
(
 `snapshotid` INTEGER,
 `path` VARCHAR,
 `metadata_json` VARCHAR
 `size` INTEGER,
 `mtime` INTEGER,
 `fileid` VARCHAR
);

CREATE INDEX `snapshotid_path` ON `filetable` (`snapshotid`, `path`);
CREATE INDEX `fileid` ON `filetable` (`fileid`);

CREATE TABLE `captable`
(
 `fileid` VARCHAR PRIMARY KEY,
 `type` INTEGER, -- 0:lit, 1:small, 2:big
 `filecap` VARCHAR
);

CREATE INDEX `filecap` ON `captable` (`filecap`);

CREATE TABLE `small_objmap`
(
 `filecap` VARCHAR PRIMARY KEY,
 `storage_index` VARCHAR,
 `offset` INTEGER,
 `size` INTEGER,
 `file_enckey` VARCHAR,
 `cthash` VARCHAR
);

CREATE TABLE `big_objmap`
(
 `filecap` VARCHAR PRIMARY KEY,
 `file_enckey` VARCHAR,
 `cthash` VARCHAR
);

CREATE TABLE `big_objmap_segments`
(
 `filecap` VARCHAR PRIMARY KEY,
 `segnum` INTEGER,
 `storage_index` VARCHAR
);


"""

class Scanner:
    def __init__(self, rootpath, dbfile):
        assert isinstance(rootpath, unicode)
        self.rootpath = os.path.abspath(rootpath)
        self.dbfile = dbfile
        self.db = dbutil.get_db(dbfile, create_version=(schema, 1),
                                synchronous="OFF")

    def scan(self):
        started = time.time()
        snapshotid = self.db.execute("INSERT INTO snapshots"
                                     " (started) VALUES (?)",
                                     (started,)).lastrowid
        (rootid, cumulative_size, cumulative_items) = \
              self.process_directory(snapshotid, u".", None)
        finished = time.time()
        self.db.execute("UPDATE snapshots"
                        " SET finished=?, rootpath=?, root_id=?"
                        " WHERE id=?",
                        (finished, self.rootpath, rootid,
                         snapshotid))
        self.db.commit()
        return (cumulative_size, cumulative_items)

    def process_directory(self, snapshotid, localpath, parentid):
        assert isinstance(localpath, unicode)
        # localpath is relative to self.rootpath
        abspath = os.path.join(self.rootpath, localpath)
        print "%sDIR: %s" % (" "*(len(localpath.split(os.sep))-1), localpath)
        s = os.stat(abspath)
        size = s.st_size # good enough for now
        name = os.path.basename(os.path.abspath(abspath))
        dirid = self.db.execute("INSERT INTO nodes"
                                " (snapshotid, parentid, name, isdir, size)"
                                " VALUES (?,?,?,?,?)",
                                (snapshotid, parentid, name, 1, size)
                                ).lastrowid
        cumulative_size = size
        cumulative_items = 1
        for child in os.listdir(abspath):
            childpath = os.path.join(localpath, child)

            if os.path.islink(os.path.join(self.rootpath, childpath)):
                pass
            elif os.path.isdir(os.path.join(self.rootpath, childpath)):
                try:
                    new_dirid, subtree_size, subtree_items = \
                               self.process_directory(snapshotid,
                                                      childpath, dirid)
                    cumulative_size += subtree_size
                    cumulative_items += subtree_items
                except OSError as e:
                    print e
                    continue
            else:
                new_fileid, file_size = self.process_file(snapshotid,
                                                          childpath, dirid)
                cumulative_size += file_size
                cumulative_items += 1

        self.db.execute("UPDATE nodes"
                        " SET cumulative_size=?, cumulative_items=?"
                        " WHERE id=?",
                        (cumulative_size, cumulative_items, dirid))
        return dirid, cumulative_size, cumulative_items

    def process_file(self, snapshotid, localpath, parentid):
        assert isinstance(localpath, unicode)
        abspath = os.path.join(self.rootpath, localpath)
        name = os.path.basename(os.path.abspath(abspath))
        print "%sFILE %s" % (" "*(len(localpath.split(os.sep))-1), name)

        s = os.stat(abspath)
        size = s.st_size
        fileid = self.db.execute("INSERT INTO nodes "
                                 "(snapshotid, parentid, name, isdir,"
                                 " size, cumulative_size, cumulative_items)"
                                 " VALUES (?,?,?,?,?,?,?)",
                                 (snapshotid, parentid, name, 0, size, size, 1)
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
    command = sys.argv[3]
    if command == "scan":
        cumulative_size, cumulative_items = s.scan()
        print "cumulative_size %d (%s)" % (cumulative_size,
                                           abbreviate_space(cumulative_size))
        print "cumulative_items %d" % cumulative_items
    elif command == "upload":
        s.upload()

if __name__ == "__main__":
    main()
