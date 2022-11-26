package barrel

import "os"

type Store struct {
	ActiveFile *os.File
	Reader     *os.File
	StaleFiles []*os.File
	Offset     int

	flockF *os.File
}
