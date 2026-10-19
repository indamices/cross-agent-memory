# Earlier debugging fix
The indexer accidentally read SQLite WAL sidecar files as text. Exclude -wal,
-shm and -journal by file type to prevent binary garbage from entering memory.
