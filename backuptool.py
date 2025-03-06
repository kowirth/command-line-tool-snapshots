#!/usr/bin/env python3

import argparse
import sqlite3
import os
import sys
import hashlib
import datetime
import shutil

# ----------------------------
# BackupTool class definition
# ----------------------------
class BackupTool:
    def __init__(self, db_path):
        self.db_path = db_path
        # Connect to the SQLite database (it will be created if it doesn't exist)
        self.conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """Initializes the database schema."""
        cur = self.conn.cursor()
        # Table to store snapshots with a timestamp
        cur.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT
            )
        ''')
        # Table to store unique file contents (blobs) using SHA-256 hash as key.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS blobs (
                hash TEXT PRIMARY KEY,
                content BLOB,
                size INTEGER
            )
        ''')
        # Table to map a snapshot to its files. Files are stored with their relative paths.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS files (
                snapshot_id INTEGER,
                path TEXT,
                blob_hash TEXT,
                PRIMARY KEY (snapshot_id, path),
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
                FOREIGN KEY (blob_hash) REFERENCES blobs(hash)
            )
        ''')
        self.conn.commit()

    def snapshot(self, target_directory):
        """
        Takes a snapshot of all files in the specified directory.
        Files are read in binary mode; only content and filename (as a relative path)
        are stored in the database.
        """
        target_directory = os.path.abspath(target_directory)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO snapshots (timestamp) VALUES (?)", (timestamp,))
        snapshot_id = cur.lastrowid

        for root, dirs, files in os.walk(target_directory):
            for file in files:
                file_path = os.path.join(root, file)
                # Compute the relative path so that the directory structure is preserved on restore.
                rel_path = os.path.relpath(file_path, target_directory)
                try:
                    with open(file_path, "rb") as f:
                        content = f.read()
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    continue
                # Calculate SHA-256 hash
                blob_hash = hashlib.sha256(content).hexdigest()
                # Insert blob if it does not exist
                cur.execute("SELECT 1 FROM blobs WHERE hash = ?", (blob_hash,))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO blobs (hash, content, size) VALUES (?, ?, ?)",
                        (blob_hash, content, len(content))
                    )
                # Insert file entry for this snapshot
                cur.execute(
                    "INSERT INTO files (snapshot_id, path, blob_hash) VALUES (?, ?, ?)",
                    (snapshot_id, rel_path, blob_hash)
                )
        self.conn.commit()
        print(f"Snapshot {snapshot_id} taken at {timestamp}")

    def list_snapshots(self):
        """Lists all snapshots with their snapshot number and timestamp."""
        cur = self.conn.cursor()
        cur.execute("SELECT id, timestamp FROM snapshots ORDER BY id")
        rows = cur.fetchall()
        if rows:
            print(f"{'SNAPSHOT':<9} TIMESTAMP")
            for row in rows:
                print(f"{row[0]:<9} {row[1]}")
        else:
            print("No snapshots found.")

    def restore(self, snapshot_id, output_directory):
        """
        Restores the state of a directory from the snapshot identified by snapshot_id.
        The directory structure and file contents are re-created exactly as stored.
        """
        cur = self.conn.cursor()
        # Check if snapshot exists
        cur.execute("SELECT id FROM snapshots WHERE id = ?", (snapshot_id,))
        if not cur.fetchone():
            print(f"Snapshot {snapshot_id} not found.")
            return

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        cur.execute("SELECT path, blob_hash FROM files WHERE snapshot_id = ?", (snapshot_id,))
        rows = cur.fetchall()
        for path, blob_hash in rows:
            out_path = os.path.join(output_directory, path)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            cur.execute("SELECT content FROM blobs WHERE hash = ?", (blob_hash,))
            blob_row = cur.fetchone()
            if not blob_row:
                print(f"Error: missing blob {blob_hash} for file {path}")
                continue
            with open(out_path, "wb") as f:
                f.write(blob_row[0])
        print(f"Snapshot {snapshot_id} restored to {output_directory}")

    def prune(self, snapshot_id):
        """
        Prunes (removes) snapshots with IDs less than or equal to snapshot_id.
        After deletion, any blob not referenced by any remaining snapshot is removed.
        This ensures that remaining snapshots are still fully restorable.
        """
        cur = self.conn.cursor()
        # Determine which snapshots will be pruned.
        cur.execute("SELECT id FROM snapshots WHERE id <= ?", (snapshot_id,))
        to_delete = [row[0] for row in cur.fetchall()]
        if not to_delete:
            print("No snapshots to prune.")
            return
        # Remove file entries for pruned snapshots.
        cur.execute("DELETE FROM files WHERE snapshot_id <= ?", (snapshot_id,))
        # Remove the snapshot records.
        cur.execute("DELETE FROM snapshots WHERE id <= ?", (snapshot_id,))
        # Remove any blobs not referenced by any remaining snapshot.
        cur.execute("DELETE FROM blobs WHERE hash NOT IN (SELECT DISTINCT blob_hash FROM files)")
        self.conn.commit()
        print(f"Pruned snapshots: {to_delete}")

    def close(self):
        """Closes the database connection."""
        self.conn.close()

# ----------------------------
# Command-line interface
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Backup Tool")
    parser.add_argument("--db", default=".backuptool.db", help="Path to database file")
    subparsers = parser.add_subparsers(dest="command")

    snapshot_parser = subparsers.add_parser("snapshot", help="Take a snapshot of a directory")
    snapshot_parser.add_argument("--target-directory", required=True, help="Directory to snapshot")

    list_parser = subparsers.add_parser("list", help="List snapshots")

    restore_parser = subparsers.add_parser("restore", help="Restore a snapshot to a directory")
    restore_parser.add_argument("--snapshot-number", type=int, required=True, help="Snapshot number to restore")
    restore_parser.add_argument("--output-directory", required=True, help="Directory to restore files into")

    prune_parser = subparsers.add_parser("prune", help="Prune snapshots up to a given snapshot")
    prune_parser.add_argument("--snapshot", type=int, required=True, help="Prune all snapshots with id <= this number")

    test_parser = subparsers.add_parser("test", help="Run automated tests")

    args = parser.parse_args()

    if args.command == "test":
        run_tests()
        return

    tool = BackupTool(args.db)
    if args.command == "snapshot":
        tool.snapshot(args.target_directory)
    elif args.command == "list":
        tool.list_snapshots()
    elif args.command == "restore":
        tool.restore(args.snapshot_number, args.output_directory)
    elif args.command == "prune":
        tool.prune(args.snapshot)
    else:
        parser.print_help()
    tool.close()

# ----------------------------
# Automated tests using unittest
# ----------------------------
import unittest
import tempfile
import filecmp

class TestBackupTool(unittest.TestCase):

    def test_snapshot_and_restore(self):
        # Create a temporary source directory with some files.
        with tempfile.TemporaryDirectory() as tmp_src, tempfile.TemporaryDirectory() as tmp_dst, tempfile.NamedTemporaryFile(delete=False) as tmp_db:
            # Create a text file.
            file1 = os.path.join(tmp_src, "file1.txt")
            with open(file1, "w") as f:
                f.write("Hello World")
            # Create a subdirectory and a binary file.
            subdir = os.path.join(tmp_src, "subdir")
            os.makedirs(subdir)
            file2 = os.path.join(subdir, "file2.bin")
            with open(file2, "wb") as f:
                f.write(os.urandom(1024))  # 1KB random binary data

            tool = BackupTool(tmp_db.name)
            tool.snapshot(tmp_src)
            snapshot_id = 1  # The first snapshot taken will have id 1.
            restore_dir = os.path.join(tmp_dst, "restore")
            tool.restore(snapshot_id, restore_dir)
            tool.close()

            # Verify that the restored files are bit-for-bit identical.
            self.assertTrue(filecmp.cmp(file1, os.path.join(restore_dir, "file1.txt"), shallow=False))
            self.assertTrue(filecmp.cmp(file2, os.path.join(restore_dir, "subdir", "file2.bin"), shallow=False))

    def test_incremental(self):
        # Check that taking a snapshot twice without changes does not duplicate blob storage.
        with tempfile.TemporaryDirectory() as tmp_src, tempfile.NamedTemporaryFile(delete=False) as tmp_db:
            file1 = os.path.join(tmp_src, "file1.txt")
            with open(file1, "w") as f:
                f.write("Test")
            tool = BackupTool(tmp_db.name)
            tool.snapshot(tmp_src)
            cur = tool.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM blobs")
            count1 = cur.fetchone()[0]
            # Take a second snapshot without changing the file.
            tool.snapshot(tmp_src)
            cur.execute("SELECT COUNT(*) FROM blobs")
            count2 = cur.fetchone()[0]
            tool.close()
            # Blob count should remain the same.
            self.assertEqual(count1, count2)

    def test_prune(self):
        # Test that pruning a snapshot removes only the specified snapshots and
        # that remaining snapshots can be restored correctly.
        with tempfile.TemporaryDirectory() as tmp_src, tempfile.NamedTemporaryFile(delete=False) as tmp_db, tempfile.TemporaryDirectory() as tmp_dst:
            file1 = os.path.join(tmp_src, "file1.txt")
            with open(file1, "w") as f:
                f.write("Snapshot1")
            tool = BackupTool(tmp_db.name)
            tool.snapshot(tmp_src)  # Snapshot 1

            # Modify the file and take another snapshot.
            with open(file1, "w") as f:
                f.write("Snapshot2")
            tool.snapshot(tmp_src)  # Snapshot 2

            # Prune snapshot 1.
            tool.prune(1)

            # Restore snapshot 2.
            restore_dir = os.path.join(tmp_dst, "restore")
            tool.restore(2, restore_dir)
            tool.close()

            # Verify that the restored file content is from snapshot 2.
            with open(os.path.join(restore_dir, "file1.txt"), "r") as f:
                content = f.read()
            self.assertEqual(content, "Snapshot2")

def run_tests():
    """Run the automated test suite."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBackupTool)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)

# ----------------------------
# Main entry point
# ----------------------------
if __name__ == '__main__':
    main()
