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
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
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

CREATE TABLE `need_to_hash`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `filetable_id` INTEGER, -- filetable row to update with fileid
 `localpath` VARCHAR,
 `mtime` INTEGER
);

CREATE TABLE `need_to_upload`
(
 `id` INTEGER PRIMARY KEY AUTOINCREMENT,
 `path` VARCHAR,
 `fileid` VARCHAR UNIQUE,
 `size` INTEGER
);
CREATE INDEX `need_to_upload_fileid` ON `need_to_upload` (`fileid`);

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
        self.prev_snapshotid, self.prev_rootid = None, None
        row = self.db.execute("SELECT * FROM snapshots"
                              " WHERE scan_finished IS NOT NULL"
                              " ORDER BY id DESC LIMIT 1").fetchone()
        if row:
            self.prev_snapshotid = row["id"]
            self.prev_rootid = row["root_id"]
        print "PREV_SNAPSHOTID", self.prev_snapshotid

    def scan(self):
        started = time.time()
        snapshotid = self.db.execute("INSERT INTO snapshots"
                                     " (started) VALUES (?)",
                                     (started,)).lastrowid
        (rootid, cumulative_size, cumulative_items) = \
              self.process_directory(snapshotid, u".", None, self.prev_rootid)
        scan_finished = time.time()
        self.db.execute("UPDATE snapshots"
                        " SET scan_finished=?, rootpath=?, root_id=?"
                        " WHERE id=?",
                        (scan_finished, self.rootpath, rootid,
                         snapshotid))
        self.db.commit()
        return (cumulative_size, cumulative_items)

    def process_directory(self, snapshotid, localpath, parentid, prevnode):
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
            " VALUES (?,?,?)",
            (snapshotid, parentid, name)
            ).lastrowid
        cumulative_size = size
        cumulative_items = 1
        for child in os.listdir(abspath):
            childpath = os.path.join(localpath, child)

            if os.path.islink(os.path.join(self.rootpath, childpath)):
                pass
            elif os.path.isdir(os.path.join(self.rootpath, childpath)):
                row = self.db.execute(
                    "SELECT * FROM dirtable"
                    " WHERE parentid=? AND name=?",
                    (prevnode, os.path.basename(childpath))).fetchone()
                try:
                    new_dirid, subtree_size, subtree_items = \
                               self.process_directory(snapshotid,
                                                      childpath, dirid,
                                                      row["id"] if row else None)
                    cumulative_size += subtree_size
                    cumulative_items += subtree_items
                except OSError as e:
                    print e
                    continue
            else:
                row = self.db.execute(
                    "SELECT * FROM filetable"
                    " WHERE parentid=? AND name=?",
                    (prevnode, os.path.basename(childpath))).fetchone()
                file_size = self.process_file(snapshotid,
                                              childpath, dirid,
                                              row["id"] if row else None)
                cumulative_size += file_size
                cumulative_items += 1

        self.db.execute("UPDATE dirtable"
                        " SET cumulative_size=?, cumulative_items=?"
                        " WHERE id=?",
                        (cumulative_size, cumulative_items,
                         dirid))
        return dirid, cumulative_size, cumulative_items

    def process_file(self, snapshotid, localpath, parentid, prevnode):
        assert isinstance(localpath, unicode)
        abspath = os.path.join(self.rootpath, localpath)
        name = os.path.basename(os.path.abspath(abspath))
        print "%sFILE %s" % (" "*(len(localpath.split(os.sep))-1), name)

        s = os.stat(abspath)
        size = s.st_size
        ftid = self.db.execute("INSERT INTO filetable"
                               " (snapshotid, parentid, name,"
                               "  size, mtime)"
                               " VALUES (?,?,?, ?,?)",
                               (snapshotid, parentid, name,
                                s.st_size, s.st_mtime)
                               ).lastrowid

        # if the file looks old (the previous snapshot had a file with the
        # same path, size, and mtime), then we're allowed to assume it hasn't
        # changed, and copy the fileid from the last snapshot
        if (prevnode and
            prevnode["size"] == size and
            prevnode["mtime"] == s.st_mtime):
            self.db.execute("UPDATE filetable SET fileid=? WHERE id=?",
                            (prevnode["fileid"], ftid))
        else:
            # otherwise, schedule it for hashing, which will produce the
            # fileid. If that fileid is not one we've previously uploaded,
            # we'll schedule it for uploading.
            self.db.execute("INSERT INTO need_to_hash"
                            " (filetable_id, localpath, mtime)"
                            " VALUES (?,?,?)",
                            (ftid, localpath, s.st_mtime))

        return size

    def hash_files(self):
        while True:
            next_batch = list(self.db.execute("SELECT * FROM need_to_hash"
                                              " ORDER BY id ASC"
                                              " LIMIT 200").fetchall())
            if not next_batch:
                return
            for row in next_batch:
                print row["localpath"].encode("utf-8")
                size = self.db.execute("SELECT size FROM filetable WHERE id=?",
                                       (row["filetable_id"],)
                                       ).fetchone()["size"]
                fileid = self.hash_fileid(row["localpath"], row["mtime"], size)
                self.db.execute("UPDATE filetable SET fileid=? WHERE id=?",
                                (row["filetable_id"], fileid))
                uploaded = self.db.execute("SELECT * FROM captable"
                                           " WHERE fileid=?",
                                           (fileid,)
                                           ).fetchone()
                need_to_upload = self.db.execute("SELECT * FROM need_to_upload"
                                                 " WHERE fileid=?",
                                                 (fileid,)).fetchone()
                if not uploaded and not need_to_upload:
                    print " need to upload"
                    path = os.path.join(self.rootpath, row["localpath"])
                    self.db.execute("INSERT INTO need_to_upload"
                                    " (path, fileid, size)"
                                    " VALUES (?,?,?)",
                                    (path, fileid, size))
            where_clause = " OR ".join(["id=?" for row in next_batch])
            values = tuple([row["id"] for row in next_batch])
            self.db.execute("DELETE FROM need_to_hash WHERE %s" % where_clause,
                            values)
            self.db.commit()

    def hash_fileid(self, localpath, mtime, size):
        # fileid will be the raw sha256 hash of the file contents. Hashing
        # files will let us efficiently handle renames (not re-uploading an
        # unmodified file that just happens to live in a different location
        # than before) as well as duplicates. We'll only hash files when we
        # don't recognize their path+mtime+size. For now, rather than pay the
        # IO cost of hashing such files, we'll just make the fileid a
        # deterministic random function of path+mtime+size.
        fileid = sha256("%s:%s:%s" % (localpath.encode("utf-8"), mtime, size)).hexdigest()
        return fileid

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

    def upload(self):
        pass

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
    elif command == "hash_files":
        s.hash_files()
    elif command == "upload":
        s.upload()

if __name__ == "__main__":
    main()
