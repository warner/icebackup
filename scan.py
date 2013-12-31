#!/usr/bin/python

import sys, os, time, base64
from hashlib import sha256
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
 `rootpath` VARCHAR,
 `started` INTEGER,
 `scan_finished` INTEGER,
 `root_id` INTEGER
);

CREATE TABLE `dirtable`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `snapshotid` INTEGER,
 `parentid` INTEGER, -- or NULL for a root
 `name` VARCHAR,
 `cumulative_size` INTEGER,
 `cumulative_items` INTEGER
);
CREATE INDEX `dirtable_snapshotid_parentid_name` ON `dirtable`
 (`snapshotid`, `parentid`, `name`);

CREATE TABLE `filetable`
(
 `snapshotid` INTEGER,
 `parentid` INTEGER NOT NULL,
 `name` VARCHAR,
 `size` INTEGER,
 `mtime` INTEGER,
 `fileid` VARCHAR -- hash of file, or random until we want efficient renames
);
CREATE INDEX `filetable_snapshotid_parentid_name` ON `filetable`
 (`snapshotid`, `parentid`, `name`);
CREATE INDEX `filetable_fileid` ON `filetable` (`fileid`);

CREATE TABLE `need_to_upload`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `path` VARCHAR,
 `fileid` VARCHAR,
 `size` INTEGER
);

CREATE TABLE `captable`
(
 `fileid` VARCHAR PRIMARY KEY,
 `type` INTEGER, -- 0:lit, 1:small, 2:big
 `filecap` VARCHAR
);

CREATE INDEX `captable_filecap` ON `captable` (`filecap`);

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
        scan_finished = time.time()
        self.db.execute("UPDATE snapshots"
                        " SET scan_finished=?, rootpath=?, root_id=?"
                        " WHERE id=?",
                        (scan_finished, self.rootpath, rootid,
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
        dirid = self.db.execute(
            "INSERT INTO dirtable"
            " (snapshotid, parentid, name)"
            " VALUES (?,?,?,?)",
            (snapshotid, parentid, name)
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

        self.db.execute("UPDATE dirtable"
                        " SET cumulative_size=?, cumulative_items=?"
                        " WHERE id=?",
                        (cumulative_size, cumulative_items,
                         dirid))
        return dirid, cumulative_size, cumulative_items

    def process_file(self, snapshotid, localpath, parentid):
        assert isinstance(localpath, unicode)
        abspath = os.path.join(self.rootpath, localpath)
        name = os.path.basename(os.path.abspath(abspath))
        print "%sFILE %s" % (" "*(len(localpath.split(os.sep))-1), name)

        s = os.stat(abspath)
        size = s.st_size
        # fileid will be the raw sha256 hash of the file contents. Hashing
        # files will let us efficiently handle renames (not re-uploading an
        # unmodified file that just happens to live in a different location
        # than before) as well as duplicates. We'll only hash files when we
        # don't recognize their path+mtime+size. For now, rather than pay the
        # IO cost of hashing such files, we'll just make the fileid a
        # deterministic random function of path+mtime+size.
        fileid = sha256("%s:%s:%s" % (localpath.encode("utf-8"), s.st_mtime, s.st_size)).hexdigest()
        self.db.execute("INSERT INTO filetable"
                        " (snapshotid, parentid, name,"
                        "  size, mtime, fileid)"
                        " VALUES (?,?,?, ?,?,?)",
                        (snapshotid, parentid, name,
                         s.st_size, s.st_mtime, fileid)
                        )

        return fileid, size

    def upload_file(self, abspath):
        return "fake filecap"
        f = open(abspath, "rb")
        h = sha256()
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
