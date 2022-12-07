package datafile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	ACTIVE_DATAFILE = "barrel_%d.db"
)

type DataFile struct {
	sync.RWMutex

	writer *os.File
	reader *os.File
	id     int

	offset int
}

// New initialises a db store for storing/reading an active db file.
// At a given time only one file can be active.
func New(dir string, index int) (*DataFile, error) {
	// If the file doesn't exist, create it, or append to the file.
	path := filepath.Join(dir, fmt.Sprintf(ACTIVE_DATAFILE, index))
	writer, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file for writing db: %w", err)
	}

	// Create a reader for reading the db file.
	reader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading db: %w", err)
	}

	// Get the offset for the current file.
	stat, err := writer.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	df := &DataFile{
		writer: writer,
		reader: reader,
		id:     index,
		offset: int(stat.Size()),
	}

	return df, nil
}

// ID returns the ID of the datafile.
func (d *DataFile) ID() int {
	return d.id
}

// Size returns the size of DB file in bytes.
func (d *DataFile) Size() (int64, error) {
	// Use stat to get file syze in bytes.
	stat, err := d.writer.Stat()
	if err != nil {
		return -1, fmt.Errorf("error fetching file stats: %v", err)
	}

	return stat.Size(), nil
}

// Sync flushes the in-memory buffers to the disk.
func (d *DataFile) Sync() error {
	return d.writer.Sync()
}

func (d *DataFile) Read(pos int, size int) ([]byte, error) {
	// Byte position to read the file from.
	start := int64(pos - size)

	// Initialise a buffer for reading data.
	record := make([]byte, size)

	// Read the file with the given offset.
	n, err := d.reader.ReadAt(record, start)
	if err != nil {
		return nil, err
	}

	// Check if the size of bytes read matches the record size.
	if n != int(size) {
		return nil, fmt.Errorf("error fetching record, invalid size")
	}

	return record, nil
}

// Write writes the record to the underlying db file.
func (d *DataFile) Write(data []byte) (int, error) {
	if _, err := d.writer.Write(data); err != nil {
		return -1, err
	}

	// Store the current size of the file.
	offset := d.offset

	// Increase the offset of the current active file.
	d.offset += len(data)

	return offset, nil
}

// Close closes the file descriptors of the underlying db file.
func (d *DataFile) Close() error {
	if err := d.writer.Close(); err != nil {
		return err
	}

	if err := d.reader.Close(); err != nil {
		return err
	}

	return nil
}
