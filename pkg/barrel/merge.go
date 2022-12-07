package barrel

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/mr-karan/barreldb/internal/datafile"
)

// MergeFiles runs the compaction process of merging log files in a single new file.
// When compaction occurs, the Bitcask database will select two or more log files to merge,
// and it will create a new log file that contains the combined data from the selected log files.
// This new log file will be used in place of the old log files, and the old log files will be deleted to reclaim disk space.
// func (b *Barrel) MergeFiles(evalInterval time.Duration) {
// 	var (
// 		evalTicker = time.NewTicker(evalInterval).C
// 	)
// 	for range evalTicker {
// 		if err := b.Merge(); err != nil {
// 			b.lo.Error("error merging files", "error", err)
// 		}
// 	}
// }

func (b *Barrel) Merge() error {
	b.Lock()
	defer b.Unlock()

	// Create a new datafile for storing the output of merged files.
	// Use a temp directory to store the file and move to main directory after merge is over.

	tmpMergeDir, err := ioutil.TempDir("", "merged")
	if err != nil {
		return err
	}
	// defer os.RemoveAll(tmpMergeDir)

	mergeDF, err := datafile.New(tmpMergeDir, 0)
	if err != nil {
		return err
	}

	// Create a new barrel instance and destroy after merge is done.
	// This new instance would help to use the `.Put` API call directly but instead of writing
	// to an active db file, it'll write to the temp merged datafile.
	tmpBarrel := &Barrel{
		opts:   b.opts,
		lo:     b.lo,
		df:     mergeDF,
		stale:  make(map[int]*datafile.DataFile, 0),
		flockF: b.flockF,
		keydir: make(KeyDir, 0),
	}

	// Loop over all active keys in the hashmap and write the updated values to merged database.
	// Since the keydir has updated values of all keys, all the old keys which are expired/deleted/overwritten
	// will be cleaned up in the merged database.
	for k := range b.keydir {
		record, err := b.get(k)
		if err != nil {
			return err
		}
		if err := tmpBarrel.put(k, record.Value, nil); err != nil {
			return err
		}
	}

	// Now remove all the existing datafiles.
	if err := b.df.Close(); err != nil {
		return err
	}

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

	tmpBarrel.Close()

	// Move the merged file to the main directory.
	os.Rename(filepath.Join(tmpMergeDir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)),
		filepath.Join(b.opts.Dir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)))

	// TODO: Produce hints file

	// TODO: Reopen bitcask by reopening file handlers on the existing files.

	return nil
}
