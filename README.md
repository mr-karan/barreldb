# barreldb
A disk based KV store (based on Bitcask implementation)

Bitcask is a log-structured hash table (LSHT) database that is designed for use as a storage engine for key-value data. It uses a log-structured approach to data management, which means that data is written to a log file and is periodically merged with other log files in the database to create new, larger log files.

The process of merging log files in Bitcask is called compaction. When compaction occurs, the Bitcask database will select two or more log files to merge, and it will create a new log file that contains the combined data from the selected log files. This new log file will be used in place of the old log files, and the old log files will be deleted to free up space.

The advantage of this approach is that it allows Bitcask to store data efficiently and to perform well even when dealing with large amounts of data. By periodically merging log files, Bitcask can avoid the need to split or resize its data files, which can be slow and expensive operations. It also allows Bitcask to perform efficient lookups of data by key, since the keys are stored in a hash table that is stored in memory.