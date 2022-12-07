package barrel

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"
)

// KeyDir represents an in-memory hash for faster lookups of the key.
// Once the key is found in the map, the additional metadata like the offset record
// and the file ID is used to extract the underlying record from the disk.
// Advantage is that this approach only requires a single disk seek of the db file
// since the position offset (in bytes) is already stored.
type KeyDir map[string]Meta

// Meta represents some additional properties for the given key.
// The actual value of the key is not stored in the in-memory hashtable.
type Meta struct {
	Timestamp  int
	RecordSize int
	RecordPos  int
	FileID     int
}

func (b *Barrel) get(k string) ([]byte, error) {
	// Check for entry in KeyDir.
	meta, ok := b.keydir[k]
	if !ok {
		return nil, fmt.Errorf("error finding data for the given key")
	}

	var (
		// Header object for decoding the binary data into it.
		header Header
	)

	// Read the file with the given offset.
	record, err := b.df.Read(meta.RecordPos, meta.RecordSize)
	if err != nil {
		return nil, fmt.Errorf("error reading data from file: %v", err)
	}

	// Decode the header.
	header.decode(record)

	// Get the offset position in record to start reading the value from.
	valPos := meta.RecordSize - int(header.ValSize)

	// Read the value from the record.
	val := record[valPos:]

	// Check if the key has already expired or not.
	// If expired, then don't return any result.
	if time.Now().Unix() > int64(header.Expiry) {
		return nil, fmt.Errorf("error finding data for the given key")
	}

	// Validate the checksum.
	if crc32.ChecksumIEEE(val) != header.Checksum {
		return nil, fmt.Errorf("invalid data: checksum does not match")
	}

	return val, nil
}

func (b *Barrel) put(k string, val []byte, expiry *time.Time) error {
	// Prepare header.
	header := Header{
		Checksum:  crc32.ChecksumIEEE(val),
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(k)),
		ValSize:   uint32(len(val)),
	}

	// Check for expiry
	if expiry != nil {
		header.Expiry = uint32(expiry.Unix())
	}

	// Prepare the record.
	record := Record{
		Key:   k,
		Value: val,
	}

	// Get a buffer from the pool for writing.
	buf := b.bufPool.Get().(*bytes.Buffer)
	defer b.bufPool.Put(buf)

	// Encode header.
	header.encode(buf)

	// Write key/value.
	buf.WriteString(k)
	buf.Write(val)

	// Append to underlying file.
	offset, err := b.df.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("error writing data to file: %v", err)
	}

	// Add entry to KeyDir.
	// We just save the value of key and some metadata for faster lookups.
	// The value is only stored in disk.
	b.keydir[k] = Meta{
		Timestamp:  int(record.Header.Timestamp),
		RecordSize: len(buf.Bytes()),
		RecordPos:  offset + len(buf.Bytes()),
		FileID:     b.df.ID(),
	}

	// Ensure filesystem's in memory buffer is flushed to disk.
	if b.opts.EnableFSync {
		if err := b.df.Sync(); err != nil {
			return fmt.Errorf("error syncing file to disk: %v", err)
		}
	}

	return nil
}

func (b *Barrel) delete(k string) error {
	// Store an empty tombstone value for the given key.
	if err := b.put(k, []byte{}, nil); err != nil {
		return err
	}

	// Delete it from the map as well.
	delete(b.keydir, k)

	return nil
}
