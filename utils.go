package barrel

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Exists returns true if the given path exists on the filesystem.
func exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}

// getDataFiles returns the list of db files in a given directory.
func getDataFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", dir))
	if err != nil {
		return nil, err
	}
	return files, nil
}

// getIDs return the sorted list of IDs extracted from the list of filenames.
func getIDs(files []string) ([]int, error) {
	ids := make([]int, 0)

	for _, f := range files {
		id, err := strconv.ParseInt((strings.TrimPrefix(strings.TrimSuffix(filepath.Base(f), ".db"), "barrel_")), 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int(id))
	}

	// Sort in increasing order.
	sort.Ints(ids)

	return ids, nil
}

// validateKV validates key/value before inserting.
func validateKV(k string, val []byte) error {
	if len(k) == 0 {
		return ErrEmptyKey
	}

	if len(k) > MaxKeySize {
		return ErrLargeKey
	}

	if len(val) > MaxValueSize {
		return ErrLargeValue
	}

	return nil
}
