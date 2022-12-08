package barrel

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mr-karan/barreldb/internal/datafile"
	"github.com/zerodha/logf"
)

const (
	LOCKFILE   = "barrel.lock"
	HINTS_FILE = "barrel.hints"
)

type Barrel struct {
	sync.Mutex

	lo      logf.Logger
	bufPool sync.Pool // Pool of byte buffers used for writing.
	opts    Opts

	keydir KeyDir                     // In-memory hashmap of all active keys.
	df     *datafile.DataFile         // Active datafile.
	stale  map[int]*datafile.DataFile // Map of older datafiles with their IDs.
	flockF *os.File                   //Lockfile to prevent multiple write access to same datafile.
}

// Opts represents configuration options for managing a datastore.
type Opts struct {
	Debug bool   // Enable debug logging.
	Dir   string // Path for storing data files.

	ReadOnly bool // Whether this datastore should be opened in a read-only mode. Only one process at a time can open it in R-W mode.

	AlwaysFSync  bool          // Should flush filesystem buffer after every right.
	SyncInterval time.Duration // Interval to sync the active file on disk.

	CompactInterval time.Duration // Interval to compact old files.

	CheckFileSizeInterval time.Duration // Interval to check the file size of the active DB.
	MaxActiveFileSize     int64         // Max size of active file in bytes. On exceeding this size it's rotated.

}

// initLogger initializes logger instance.
func initLogger(debug bool) logf.Logger {
	opts := logf.Opts{EnableCaller: true}
	if debug {
		opts.Level = logf.DebugLevel
	}
	return logf.New(opts)
}

// Init initialises a datastore for storing data.
func Init(opts Opts) (*Barrel, error) {

	var (
		lo     = initLogger(opts.Debug)
		index  = 0
		flockF *os.File
		stale  = map[int]*datafile.DataFile{}
	)

	// Load existing datafiles
	files, err := getDataFiles(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("error loading data files: %w", err)
	}

	if len(files) > 0 {
		// Get the existing ids.
		ids, err := getIDs(files)
		if err != nil {
			return nil, fmt.Errorf("error parsing ids for existing files: %w", err)
		}

		// Increment the index to write to a new datafile.
		index = ids[len(ids)-1] + 1

		// Add all older datafiles to the list of stale files.
		for _, idx := range ids {
			df, err := datafile.New(opts.Dir, idx)
			if err != nil {
				return nil, err
			}
			stale[idx] = df
		}
	}

	// Initialise a db store.
	df, err := datafile.New(opts.Dir, index)
	if err != nil {
		return nil, err
	}

	// If not running in a read only mode then create a lockfile to ensure only one process writes to the db directory.
	if !opts.ReadOnly {
		// Check if a lockfile already exists.
		if exists(LOCKFILE) {
			return nil, fmt.Errorf("a lockfile already exists inside dir")
		} else {
			flockF, err = createFlockFile(LOCKFILE)
			if err != nil {
				return nil, fmt.Errorf("error creating lockfile: %w", err)
			}
		}
	}

	// Initialise an empty keydir.
	keydir := make(KeyDir, 0)

	// Check if a hints file already exists and then use that to populate the hashtable.
	hintsPath := filepath.Join(opts.Dir, HINTS_FILE)
	if exists(hintsPath) {
		if err := keydir.Decode(hintsPath); err != nil {
			return nil, fmt.Errorf("error populating hashtable from hints file: %w", err)
		}
	}

	// Initialise barrel.
	barrel := &Barrel{
		opts:   opts,
		lo:     lo,
		df:     df,
		stale:  stale,
		flockF: flockF,
		keydir: keydir,
		bufPool: sync.Pool{New: func() any {
			return bytes.NewBuffer([]byte{})
		}},
	}

	// Spawn a goroutine which runs in background and compacts all datafiles in a new single datafile.
	if opts.CompactInterval == 0 {
		opts.CompactInterval = time.Hour * 6
	}
	go barrel.RunCompaction(opts.CompactInterval)

	// Spawn a goroutine which checks for the file size of the active file at periodic interval.
	if barrel.opts.CheckFileSizeInterval == 0 {
		barrel.opts.CheckFileSizeInterval = time.Minute * 1
	}
	go barrel.ExamineFileSize(barrel.opts.CheckFileSizeInterval)

	// Spawn a goroutine which flushes the file to disk periodically.
	if !barrel.opts.AlwaysFSync && barrel.opts.SyncInterval == 0 {
		barrel.opts.SyncInterval = time.Minute * 1
	}
	if barrel.opts.SyncInterval > 0 {
		go barrel.SyncFile(barrel.opts.SyncInterval)
	}

	return barrel, nil
}

// Shutdown closes all the open file descriptors and removes any file locks.
// If non running in a read-only mode, it's essential to call close so that it
// removes any file locks on the database directory. Not calling close will prevent
// future startups until it's removed manually.
func (b *Barrel) Shutdown() {
	b.Lock()
	defer b.Unlock()

	// Generate a hints file.
	if err := b.generateHints(); err != nil {
		b.lo.Error("error generating hints file", "error", err)
	}

	// Close all active file handlers.
	if err := b.df.Close(); err != nil {
		b.lo.Error("error closing active db file", "error", err, "id", b.df.ID())
	}

	// Close all stale datafiles as well.
	for _, df := range b.stale {
		if err := df.Close(); err != nil {
			b.lo.Error("error closing active db file", "error", err, "id", df.ID())
		}
	}

	// Cleanup the lock file.
	if !b.opts.ReadOnly {
		if err := destroyFlockFile(b.flockF); err != nil {
			b.lo.Error("error destroying lock file", "error", err)
		}
	}
}

// Put takes a key and value and encodes the data in bytes and writes to the db file.
// It also stores the key with some metadata in memory.
// This metadata helps for faster reads as the last position of the file is recorded so only
// a single disk seek is required to read value.
func (b *Barrel) Put(k string, val []byte) error {
	b.Lock()
	defer b.Unlock()

	if b.opts.ReadOnly {
		return fmt.Errorf("put operation now allowed in read-only mode")
	}

	b.lo.Debug("storing data", "key", k, "val", val)
	return b.put(b.df, k, val, nil)
}

// PutEx is same as Put but also takes an additional expiry time.
func (b *Barrel) PutEx(k string, val []byte, ex time.Duration) error {
	b.Lock()
	defer b.Unlock()

	// Add the expiry to the current time.
	expiry := time.Now().Add(ex)

	if b.opts.ReadOnly {
		return fmt.Errorf("put operation now allowed in read-only mode")
	}

	b.lo.Debug("storing data with expiry", "key", k, "val", val, "expiry", ex.String())
	return b.put(b.df, k, val, &expiry)
}

// Get takes a key and finds the metadata in the in-memory hashtable (Keydir).
// Using the offset present in metadata it finds the record in the datafile with a single disk seek.
// It further decodes the record and returns the value as a byte array for the given key.
func (b *Barrel) Get(k string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()

	b.lo.Debug("fetching data", "key", k)
	record, err := b.get(k)
	if err != nil {
		return nil, err
	}

	// If expired, then don't return any result.
	if record.isExpired() {
		return nil, fmt.Errorf("invalid key: key has expired")
	}

	// If invalid checksum, return error.
	if !record.isValidChecksum() {
		return nil, fmt.Errorf("invalid data: checksum does not match")
	}

	return record.Value, nil
}

// Delete creates a tombstone record for the given key. The tombstone value is simply an empty byte array.
// Actual deletes happen in background when merge is called.
// Since the file is opened in append-only mode, the new value of the key
// is overwritten both on disk and in memory as a tombstone record.
func (b *Barrel) Delete(k string) error {
	b.Lock()
	defer b.Unlock()

	if b.opts.ReadOnly {
		return fmt.Errorf("delete operation now allowed in read-only mode")
	}

	b.lo.Debug("deleting key", "key", k)
	return b.delete(k)
}

// List iterates over all keys and returns the list of keys.
func (b *Barrel) List() []string {
	b.Lock()
	defer b.Unlock()

	keys := make([]string, len(b.keydir))

	for k := range b.keydir {
		keys = append(keys, k)
	}

	return keys
}

// Len iterates over all keys and returns the total number of keys.
func (b *Barrel) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.keydir)
}

// Fold iterates over all keys and calls the given function for each key.
func (b *Barrel) Fold(fn func(k string) error) error {
	b.Lock()
	defer b.Unlock()

	// Call fn for each key.
	for k := range b.keydir {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// Sync calls fsync(2) on the active data file.
func (b *Barrel) Sync() error {
	b.Lock()
	defer b.Unlock()

	return b.df.Sync()
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
