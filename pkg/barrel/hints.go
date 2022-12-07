package barrel

import "path/filepath"

// GenerateHints encodes the contents of the in-memory hashtable
// as `gob` and writes the data to a hints file.
func (b *Barrel) GenerateHints() error {
	b.Lock()
	defer b.Unlock()

	path := filepath.Join(b.opts.Dir, HINTS_FILE)
	if err := b.keydir.Encode(path); err != nil {
		return err
	}

	return nil
}
