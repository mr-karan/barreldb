## TODO

### Initialising

barrel.Open(dir="/data/barrel")

Create a `barrel.db` file inside `/data/barrel` which is the working data directory.

- timer.Timer -> check if file needs to be rotated (5MB)
- rename current active file
- create new file

- timer.Timer -> merge all these files 30 minutes
- loop over all inactive files
- delete records not required


.Put("hello") -> "world"

.Put("hello) -> "bye"


### Writing

- [x] Encode the header
- [x] Flush to a file
- [ ] Add Expiry
- [ ] Add Checksum
- [x] Organize methods as Encoder/Decoder package
- [x] Add KeyDir struct
  - [x] Get the file offset and add it to the hashmap

### Reading

- [x] Check in keydir
- [x] decode and return to user 

### Background

- [ ] Merge old files
- [ ] Hints file
- [ ] GC cleanup of old/expired/deleted keys

### Starting program

- [ ] Load data from hints file for faster boot time

## Test Cases
