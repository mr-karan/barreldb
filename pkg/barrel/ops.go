package barrel

import (
	"bytes"
	"fmt"
	"time"
)

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

	return record[valPos:], nil
}

func (b *Barrel) put(k string, val []byte) error {
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
		FileID:     "TODO",
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
	if err := b.put(k, []byte{}); err != nil {
		return err
	}

	// Delete it from the map as well.
	delete(b.keydir, k)

	return nil
}
