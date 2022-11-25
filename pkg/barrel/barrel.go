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
	BARREL_LOCKFILE = "barrel.lock"
)

type Barrel struct {
	sync.Mutex

	activeFile string
	file       *os.File
	reader     *os.File
	keydir     KeyDir
	offset     int
}

func Init(dir string) (*Barrel, error) {
	// If the file doesn't exist, create it, or append to the file.
	activeFile := filepath.Join(dir, ACTIVE_DATAFILE)
	f, err := os.OpenFile(activeFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file handler: %v", err)
	}

	// Use stat to get file syze in bytes.
	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	// Create an mmap reader for reading the db file.
	reader, err := os.Open(activeFile)
	if err != nil {
		return nil, fmt.Errorf("error openning mmap for db: %v", err)
	}

	// TODO: Do lockfile shenanigans. Ensure only one process can write to barrel.db at a time.

	barrel := &Barrel{
		activeFile: activeFile,
		file:       f,
		reader:     reader,
		keydir:     make(KeyDir, 0),
		offset:     int(stat.Size()),
	}

	return barrel, nil
}

func (b *Barrel) Close() {
	b.file.Close()
}

func (b *Barrel) Put(k string, val []byte) error {
	b.Lock()
	defer b.Unlock()

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
	if _, err := b.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("error writing data to file: %v", err)
	}

	// Add entry to KeyDir.
	// We just save the value of key and some metadata for faster lookups.
	// The value is only stored in disk.
	b.keydir[k] = Meta{
		Timestamp:  int(record.Header.Timestamp),
		RecordSize: len(buf.Bytes()),
		FileID:     "TODO",
	}

	// Increase the offset of the current active file.
	b.offset += len(buf.Bytes())

	// Ensure filesystem's in memory buffer is flushed to disk.
	if err := b.file.Sync(); err != nil {
		return fmt.Errorf("error syncing file to disk: %v", err)
	}

	return nil
}

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
		position = int64(b.offset - meta.RecordSize)
	)

	// Initialise a buffer for reading data.
	record := make([]byte, meta.RecordSize)

	// Read the file with the given offset.
	n, err := b.reader.ReadAt(record, position)
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
