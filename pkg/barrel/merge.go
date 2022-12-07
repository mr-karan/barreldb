package barrel

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/mr-karan/barreldb/internal/datafile"
)

// Merge is the process of merging all datafiles in a single file.
// In this process, all the expired/deleted keys are cleaned up and old files
// are removed from the disk.
func (b *Barrel) merge() error {
	b.Lock()
	defer b.Unlock()

	// Create a new datafile for storing the output of merged files.
	// Use a temp directory to store the file and move to main directory after merge is over.
	tmpMergeDir, err := ioutil.TempDir("", "merged")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpMergeDir)

	mergeDF, err := datafile.New(tmpMergeDir, 0)
	if err != nil {
		return err
	}

	activeDF := b.df

	// Replace with a new df object.
	b.df = mergeDF

	// Loop over all active keys in the hashmap and write the updated values to merged database.
	// Since the keydir has updated values of all keys, all the old keys which are expired/deleted/overwritten
	// will be cleaned up in the merged database.
	for k := range b.keydir {
		record, err := b.get(k)
		if err != nil {
			return err
		}
		if err := b.put(k, record.Value, nil); err != nil {
			return err
		}
	}

	// Now close all the existing datafile handlers.
	for _, df := range b.stale {
		if err := df.Close(); err != nil {
			b.lo.Error("error closing df", "id", df.ID(), "error", err)
			continue
		}
	}
	activeDF.Close()

	// Delete the existing .db files
	err = filepath.Walk(b.opts.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".db" {
			err := os.Remove(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Move the merged file to the main directory.
	os.Rename(filepath.Join(tmpMergeDir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)),
		filepath.Join(b.opts.Dir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)))

	return nil
}
