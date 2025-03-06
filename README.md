How to Build, Test, and Run

Build/Setup:

Ensure you have Python 3 installed.

Save the script as backuptool.py and make it executable:
chmod +x backuptool.py

Run a Snapshot:
./backuptool.py snapshot --target-directory=/path/to/your/files

List Snapshots:
./backuptool.py list

Restore a Snapshot:
./backuptool.py restore --snapshot-number=1 --output-directory=./restore_dir

Prune Old Snapshots:
./backuptool.py prune --snapshot=1

Run Automated Tests:
./backuptool.py test

Here are the SQL commands to query the database

Open the database in SQLite:
sqlite3 .backuptool.db

List all tables in the database:
.tables

View the schema of all tables:
.schema

Query the snapshots table:
SELECT * FROM snapshots;

List all files associated with a specific snapshot (e.g., snapshot 1):
SELECT path, blob_hash FROM files WHERE snapshot_id = 1;
