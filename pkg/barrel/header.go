package barrel

import (
	"bytes"
	"encoding/binary"
)

const (
	HEADER_SIZE = 12
)

/*
Record is a binary representation of how each record is persisted in the disk.
The first three fields have a fixed size of 4 bytes (so 4+4+4=12 bytes fixed width "Header").
Key size = 4 bytes which means tha max size of key can be (2^32)-1 = ~4.29GB.
Key size = 4 bytes which means tha max size of value can be (2^32)-1 = ~4.29GB.
Each entry cannot exceed more than ~8.6GB as a theoretical limit.
In a practical sense, this is also constrained by the memory of the underlying VM
where this program would run.
------------------------------------------------------
| time(4) | key_size(4) | val_size(4) | key | val    |
------------------------------------------------------
*/
type Record struct {
	Header Header
	Key    string
	Value  []byte
}

// Header represents the fixed width fields present at the start of every record.
type Header struct {
	// TODO: Add Expiry and CRC.
	Timestamp uint32
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