package barrel

import (
	"time"

	"github.com/mr-karan/barreldb/internal/datafile"
)

// ExamineFileSize runs cleanup operations at every configured interval.
// It examines the file size of the active db file and marks it as stale
// if the file size exceeds the configured size.
// Additionally it runs a merge operation which compacts the stale files.
// This produces a hints file as well which is used for faster startup time.
func (b *Barrel) ExamineFileSize(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		if err := b.rotateDF(); err != nil {
			b.lo.Printf("error rotating db file: %s\n", err)
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
	if size < b.opts.MaxFileSize {
		return nil
	}

	// Close existing FDs before moving the file.
	if err := b.df.Close(); err != nil {
		return err
	}

	oldID := b.df.ID()

	// Add this datafile to list of stale files.
	b.stale[oldID] = b.df

	// Create a new datafile.
	df, err := datafile.New(b.opts.Dir, oldID+1)
	if err != nil {
		return err
	}

	// Replace with a new instance of datafile.
	b.df = df

	return nil
}
