package barrel

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/mr-karan/barreldb/internal/datafile"
)

func (b *Barrel) get(k string) (Record, error) {
	// Check for entry in KeyDir.
	meta, ok := b.keydir[k]
	if !ok {
		return Record{}, fmt.Errorf("error finding data for the given key")
	}

	var (
		// Header object for decoding the binary data into it.
		header Header
		reader *datafile.DataFile
	)

	// Set the current file ID as the default.
	reader = b.df

	// Check if the ID is different from the current ID.
	if meta.FileID != b.df.ID() {
		reader, ok = b.stale[meta.FileID]
		if !ok {
			return Record{}, fmt.Errorf("error looking up for the db file for the given id: %d", meta.FileID)
		}
		reader.Open()
		defer reader.Close()
	}

	// Read the file with the given offset.
	data, err := reader.Read(meta.RecordPos, meta.RecordSize)
	if err != nil {
		return Record{}, fmt.Errorf("error reading data from file: %v", err)
	}

	// Decode the header.
	header.decode(data)

	var (
		// Get the offset position in record to start reading the value from.
		valPos = meta.RecordSize - int(header.ValSize)
		// Read the value from the record.
		val = data[valPos:]
	)

	record := Record{
		Header: header,
		Key:    k,
		Value:  val,
	}

	return record, nil
}

func (b *Barrel) put(k string, val []byte, expiry *time.Time) error {
	// Prepare header.
	header := Header{
		Checksum:  crc32.ChecksumIEEE(val),
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(k)),
		ValSize:   uint32(len(val)),
	}

	// Check for expiry.
	if expiry != nil {
		header.Expiry = uint32(expiry.Unix())
	} else {
		header.Expiry = 0
	}

	// Prepare the record.
	record := Record{
		Key:   k,
		Value: val,
	}

	// Get the buffer from the pool for writing data.
	buf := b.bufPool.Get().(*bytes.Buffer)
	defer b.bufPool.Put(buf)
	// Resetting the buffer is important since the length of bytes written should be reset on each `set` operation.
	defer buf.Reset()

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
