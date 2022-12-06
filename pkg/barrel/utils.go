package barrel

import (
	"os"
)

// Exists returns true if the given path exists on the filesystem.
func Exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}
