package barrel

import (
	"encoding/gob"
	"os"
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

// Encode encodes the map to a gob file.
// This is typically used to generate a hints file.
// Caller of this program should ensure to lock/unlock the map before calling.
func (k *KeyDir) Encode(fPath string) error {
	// Create a file for storing gob data.
	file, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new gob encoder.
	encoder := gob.NewEncoder(file)

	// Encode the map and save it to the file.
	err = encoder.Encode(k)
	if err != nil {
		return err
	}

	return nil
}

func (k *KeyDir) Decode(fPath string) error {
	// Open the file for decoding gob data.
	file, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new gob decoder.
	decoder := gob.NewDecoder(file)

	// Decode the file to the map.
	err = decoder.Decode(k)
	if err != nil {
		return err
	}

	return nil
}
