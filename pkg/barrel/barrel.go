package barrel

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	ACTIVE_DATAFILE = "barrel.db"
	LOCKFILE        = "barrel.lock"
)

type Barrel struct {
	sync.Mutex

	opts   Opts
	store  *Store
	keydir KeyDir
}

// Opts represents configuration options for managing a datastore.
type Opts struct {
	ReadOnly    bool // Whether this datastore should be opened in a read-only mode. Only one process at a time can open it in R-W mode.
	MaxFileSize int  // Max size of active file. On exceeding this size it's rotated.
	EnableFSync bool // Should flush filesystem buffer after every right.
}

// Init initialises a datastore for storing data.
func Init(dir string, opts Opts) (*Barrel, error) {
	// If the file doesn't exist, create it, or append to the file.
	activeFPath := filepath.Join(dir, ACTIVE_DATAFILE)
	activeF, err := os.OpenFile(activeFPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file handler: %v", err)
	}

	// Use stat to get file syze in bytes.
	stat, err := activeF.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	// Create a reader for reading the db file.
	reader, err := os.Open(activeFPath)
	if err != nil {
		return nil, fmt.Errorf("error openning mmap for db: %v", err)
	}

	var flockF *os.File
	// If not running in a read only mode then create a lockfile to ensure only one process writes to the db directory.
	if !opts.ReadOnly {
		// Check if a lockfile already exists.
		if Exists(LOCKFILE) {
			return nil, fmt.Errorf("a lockfile already exists inside dir")
		} else {
			flockF, err = CreateFlockFile(LOCKFILE)
			if err != nil {
				return nil, fmt.Errorf("error creating lockfile: %w", err)
			}
		}
	}

	// Create a datastore.
	store := &Store{
		ActiveFile: activeF,
		Reader:     reader,
		StaleFiles: make([]*os.File, 0),
		Offset:     int(stat.Size()),
		flockF:     flockF,
	}

	// Initialise barrel.
	barrel := &Barrel{
		opts:   opts,
		store:  store,
		keydir: make(KeyDir, 0),
	}

	return barrel, nil
}

// Close closes all the open file descriptors and removes any file locks.
// If non running in a read-only mode, it's essential to call close so that it
// removes any file locks on the database directory. Not calling close will prevent
// future startups until it's removed manually.
func (b *Barrel) Close() {
	b.store.ActiveFile.Close()
	b.store.Reader.Close()

	if !b.opts.ReadOnly {
		_ = DestroyFlockFile(b.store.flockF)
	}
}

// Put takes a key and value and encodes the data in bytes and writes to the db file.
// It also stores the key with some metadata in memory.
// This metadata helps for faster reads as the last position of the file is recorded so only
// a single disk seek is required to read value.
func (b *Barrel) Put(k string, val []byte) error {
	b.Lock()
	defer b.Unlock()

	if b.opts.ReadOnly {
		return fmt.Errorf("put operation now allowed in read-only mode")
	}

	// Prepare header.
	header := Header{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(k)),
		ValSize:   uint32(len(val)),
	}

	// Prepare the record.
	record := Record{
		Key:   k,
		Value: val,
	}

	// Create a buffer for writing data to it.
	// TODO: Create a buffer pool.
	buf := bytes.NewBuffer([]byte{})

	// Encode header.
	header.encode(buf)

	// Write key/value.
	buf.WriteString(k)
	buf.Write(val)

	// Append to underlying file.
	if _, err := b.store.ActiveFile.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("error writing data to file: %v", err)
	}

	// Add entry to KeyDir.
	// We just save the value of key and some metadata for faster lookups.
	// The value is only stored in disk.
	b.keydir[k] = Meta{
		Timestamp:  int(record.Header.Timestamp),
		RecordSize: len(buf.Bytes()),
		RecordPos:  b.store.Offset + len(buf.Bytes()),
		FileID:     "TODO",
	}

	// Increase the offset of the current active file.
	b.store.Offset += len(buf.Bytes())

	// Ensure filesystem's in memory buffer is flushed to disk.
	if b.opts.EnableFSync {
		if err := b.store.ActiveFile.Sync(); err != nil {
			return fmt.Errorf("error syncing file to disk: %v", err)
		}
	}

	return nil
}

// Get takes a key and finds the metadata in memory.
// Using the offset present in metadata it finds the record in the datafile with a single disk seek.
// It further decodes the record and returns the value for the given key.
func (b *Barrel) Get(k string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()

	// Check for entry in KeyDir.
	meta, ok := b.keydir[k]
	if !ok {
		return nil, fmt.Errorf("error finding data for the given key")
	}

	var (
		// Header object for decoding the binary data into it.
		header Header
		// Position to read the file from.
		position = int64(meta.RecordPos - meta.RecordSize)
	)

	// Initialise a buffer for reading data.
	record := make([]byte, meta.RecordSize)

	// Read the file with the given offset.
	n, err := b.store.Reader.ReadAt(record, position)
	if err != nil {
		return nil, fmt.Errorf("error reading data from file: %v", err)
	}

	// Check if the size of bytes read matches the record size.
	if n != int(meta.RecordSize) {
		return nil, fmt.Errorf("error fetching record, invalid size")
	}

	// Decode the header.
	header.decode(record)

	// Get the offset position in record to start reading the value from.
	valPos := meta.RecordSize - int(header.ValSize)

	return record[valPos:], nil
}

// List iterates over all keys and returns the list of keys.
func (b *Barrel) List() []string {
	keys := make([]string, len(b.keydir))
	for k := range b.keydir {
		keys = append(keys, k)
	}
	return keys
}
