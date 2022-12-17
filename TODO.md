## TODO

These are my rough notes about WIP implementations and backlog.

### Initialising

barrel.Open(dir="/data/barrel")

Create a `barrel.db` file inside `/data/barrel` which is the working data directory.

### Writing

- [x] Encode the header
- [x] Flush to a file
- [x] Add Expiry
- [x] Add Checksum
- [x] Organize methods as Encoder/Decoder package
- [x] Add KeyDir struct
- [x] Get the file offset and add it to the hashmap

### Reading

- [x] Check in keydir
- [x] decode and return to user 

### Background

- [x] Merge old files
- [x] Hints file
- [x] GC cleanup of old/expired/deleted keys
- [x] Compaction routine
- [x] Rotate file if size increases
### Starting program

- [x] Load data from hints file for faster boot time

### Raft

Availability with N+1 node with raft

- [ ] Explore hashicorp/raft

### Test Cases

- [x] Init
- [x] Put
- [x] Set
- [x] Delete
- [x] Close
- [ ] Merge
- [ ] Hints file
- [ ] Rotate size
