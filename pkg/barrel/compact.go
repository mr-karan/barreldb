package barrel

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mr-karan/barreldb/internal/datafile"
)

// ExamineFileSize checks for file size at a periodic interval.
// It examines the file size of the active db file and marks it as stale
// if the file size exceeds the configured size.
func (b *Barrel) ExamineFileSize(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		if err := b.rotateDF(); err != nil {
			b.lo.Error("error rotating db file", "error", err)
		}
	}
}

// RunCompaction runs cleanup process to compact the keys and cleanup
// dead/expired keys at a periodic interval. This helps to save disk space
// and merge old inactive db files in a single file. It also generates a hints file
// which helps in caching all the keys during a cold start.
func (b *Barrel) RunCompaction(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		b.Lock()

		if err := b.cleanupExpired(); err != nil {
			b.lo.Error("error removing expired keys", "error", err)
		}
		if err := b.merge(); err != nil {
			b.lo.Error("error merging old files", "error", err)
		}
		if err := b.generateHints(); err != nil {
			b.lo.Error("error generating hints file", "error", err)
		}

		b.Unlock()
	}
}

// SyncFile checks for file size at a periodic interval.
// It examines the file size of the active db file and marks it as stale
// if the file size exceeds the configured size.
func (b *Barrel) SyncFile(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		if err := b.Sync(); err != nil {
			b.lo.Error("error syncing db file to disk", "error", err)
		}
	}
}

// rotateDF checks if the active file size has crossed the threshold
// of max allowed file size. If it has, it replaces the open file descriptors
// pointing to that file with a new file and adds the current file to list of
// stale files.
func (b *Barrel) rotateDF() error {
	b.Lock()
	defer b.Unlock()

	size, err := b.df.Size()
	if err != nil {
		return err
	}

	// If the file is below the threshold of max size, do no action.
	b.lo.Debug("checking if db file has exceeded max_size", "current_size", size, "max_size", b.opts.maxActiveFileSize)
	if size < b.opts.maxActiveFileSize {
		return nil
	}

	oldID := b.df.ID()

	// Add this datafile to list of stale files.
	b.stale[oldID] = b.df

	// Create a new datafile.
	df, err := datafile.New(b.opts.dir, oldID+1)
	if err != nil {
		return err
	}

	// Replace with a new instance of datafile.
	b.df = df

	return nil
}

// generateHints encodes the contents of the in-memory hashtable
// as `gob` and writes the data to a hints file.
func (b *Barrel) generateHints() error {
	path := filepath.Join(b.opts.dir, HINTS_FILE)
	if err := b.keydir.Encode(path); err != nil {
		return err
	}

	return nil
}

// cleanupExpired removes the expired keys.
func (b *Barrel) cleanupExpired() error {
	// Iterate over all keys and delete all keys which are expired.
	for k := range b.keydir {
		record, err := b.get(k)
		if err != nil {
			b.lo.Error("error fetching key", "key", k, "error", err)
			continue
		}
		if record.isExpired() {
			b.lo.Debug("deleting key since it's expired", "key", k)
			// Delete the key.
			if err := b.delete(k); err != nil {
				b.lo.Error("error deleting key", "key", k, "error", err)
				continue
			}
		}
	}

	return nil
}

// Merge is the process of merging all datafiles in a single file.
// In this process, all the expired/deleted keys are cleaned up and old files
// are removed from the disk.
func (b *Barrel) merge() error {
	var (
		mergefsync bool
	)

	// There should be atleast 2 old files to merge.
	if len(b.stale) < 2 {
		return nil
	}

	// Create a new datafile for storing the output of merged files.
	// Use a temp directory to store the file and move to main directory after merge is over.
	tmpMergeDir, err := os.MkdirTemp("", "merged")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpMergeDir)

	mergeDF, err := datafile.New(tmpMergeDir, 0)
	if err != nil {
		return err
	}

	// Disable fsync for merge process and manually fsync at the end of merge.
	if b.opts.alwaysFSync {
		mergefsync = true
		b.opts.alwaysFSync = false
	}

	// Loop over all active keys in the hashmap and write the updated values to merged database.
	// Since the keydir has updated values of all keys, all the old keys which are expired/deleted/overwritten
	// will be cleaned up in the merged database.

	for k := range b.keydir {
		record, err := b.get(k)
		if err != nil {
			return err
		}
		if err := b.put(mergeDF, k, record.Value, nil); err != nil {
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

	// Reset the old map.
	b.stale = make(map[int]*datafile.DataFile, 0)

	// Delete the existing .db files
	err = filepath.Walk(b.opts.dir, func(path string, info os.FileInfo, err error) error {
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
		filepath.Join(b.opts.dir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)))

	// Set the merged DF as the active DF.
	b.df = mergeDF

	if mergefsync {
		b.opts.alwaysFSync = true
		b.df.Sync()
	}

	return nil
}
