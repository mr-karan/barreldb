package barrel

import (
	"time"
)

const (
	defaultSyncInterval      = time.Minute * 1
	defaultCompactInterval   = time.Hour * 6
	defaultFileSizeInterval  = time.Minute * 1
	defaultMaxActiveFileSize = int64(1 << 32) // 4GB.
)

// Options represents configuration options for managing a datastore.
type Options struct {
	debug                 bool           // Enable debug logging.
	dir                   string         // Path for storing data files.
	readOnly              bool           // Whether this datastore should be opened in a read-only mode. Only one process at a time can open it in R-W mode.
	alwaysFSync           bool           // Should flush filesystem buffer after every right.
	syncInterval          *time.Duration // Interval to sync the active file on disk.
	compactInterval       time.Duration  // Interval to compact old files.
	checkFileSizeInterval time.Duration  // Interval to check the file size of the active DB.
	maxActiveFileSize     int64          // Max size of active file in bytes. On exceeding this size it's rotated.
}

// Config is a function on the Options for barreldb.
// These are used to configure particular options.
type Config func(*Options) error

func DefaultOptions() *Options {
	return &Options{
		debug:                 false,
		dir:                   ".",
		readOnly:              false,
		alwaysFSync:           false,
		maxActiveFileSize:     defaultMaxActiveFileSize,
		compactInterval:       defaultCompactInterval,
		checkFileSizeInterval: defaultFileSizeInterval,
	}
}

func WithDebug() Config {
	return func(o *Options) error {
		o.debug = true
		return nil
	}
}

func WithDir(dir string) Config {
	return func(o *Options) error {
		o.dir = dir
		return nil
	}
}

func WithReadOnly() Config {
	return func(o *Options) error {
		o.readOnly = true
		return nil
	}
}

func WithAlwaysSync() Config {
	return func(o *Options) error {
		o.alwaysFSync = true
		return nil
	}
}

func WithAutoSync() Config {
	return func(o *Options) error {
		o.alwaysFSync = false
		d := defaultSyncInterval
		o.syncInterval = &d
		return nil
	}
}

func WithBackgrondSync(interval time.Duration) Config {
	return func(o *Options) error {
		o.alwaysFSync = false
		o.syncInterval = &interval
		return nil
	}
}

func WithCompactInterval(interval time.Duration) Config {
	return func(o *Options) error {
		o.compactInterval = interval
		return nil
	}
}

func WithCheckFileSizeInterval(interval time.Duration) Config {
	return func(o *Options) error {
		o.checkFileSizeInterval = interval
		return nil
	}
}

func WithMaxActiveFileSize(size int64) Config {
	return func(o *Options) error {
		o.maxActiveFileSize = size
		return nil
	}
}
