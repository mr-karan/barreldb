package barrel

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	MaxKeySize   = 1<<32 - 1
	MaxValueSize = 1<<32 - 1
)

/*
Record is a binary representation of how each record is persisted in the disk.
Header represents how the record is stored and some metadata with it.
For storing CRC checksum hash, timestamp and expiry of record, each field uses 4 bytes. (uint32 == 32 bits).
The next field stores the max size of the key which is also represented with uint32. So the max size of the key
can not be more than 2^32-1 which is ~ 4.3GB.
The next field stores the max size of the value which is also represented with unint32. Max size of value can not be more
than 2^32-1 which is ~ 4.3GB.

Each entry cannot exceed more than ~8.6GB as a theoretical limit.
In a practical sense, this is also constrained by the memory of the underlying VM
where this program would run.

Representation of the record stored on disk.
------------------------------------------------------------------------------
| crc(4) | time(4) | expiry (4) | key_size(4) | val_size(4) | key | val      |
------------------------------------------------------------------------------
*/
type Record struct {
	Header Header
	Key    string
	Value  []byte
}

// Header represents the fixed width fields present at the start of every record.
type Header struct {
	Checksum  uint32
	Timestamp uint32
	Expiry    uint32
	KeySize   uint32
	ValSize   uint32
}

// Encode takes a byte buffer, encodes the value of header and writes to the buffer.
func (h *Header) encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

// Decode takes a record object decodes the binary value the buffer.
func (h *Header) decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}

// isExpired returns true if the key has already expired.
func (r *Record) isExpired() bool {
	// If no expiry is set, this value will be 0.
	if r.Header.Expiry == 0 {
		return false
	}
	return time.Now().Unix() > int64(r.Header.Expiry)
}

// isValidChecksum returns true if the checksum of the value matches what is stored in the header.
func (r *Record) isValidChecksum() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Header.Checksum
}
