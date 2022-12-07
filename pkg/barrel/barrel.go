package barrel

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mr-karan/barreldb/internal/datafile"
	"github.com/zerodha/logf"
)

const (
	LOCKFILE = "barrel.lock"
)

type Barrel struct {
	sync.Mutex

	lo      logf.Logger
	bufPool *sync.Pool // Pool of byte buffers used for writing.
	opts    Opts

	keydir KeyDir                     // In-memory hashmap of all active keys.
	df     *datafile.DataFile         // Active datafile.
	stale  map[int]*datafile.DataFile // Map of older datafiles with their IDs.
	flockF *os.File                   //Lockfile to prevent multiple write access to same datafile.
}

// Opts represents configuration options for managing a datastore.
type Opts struct {
	Debug         bool          // Enable debug logging.
	Dir           string        // Path for storing data files.
	ReadOnly      bool          // Whether this datastore should be opened in a read-only mode. Only one process at a time can open it in R-W mode.
	MergeInterval time.Duration // Interval to compact old files.
	MaxFileSize   int64         // Max size of active file in bytes. On exceeding this size it's rotated.
	EnableFSync   bool          // Should flush filesystem buffer after every right.
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

	// TODO: Check for stale files and create an index automatically.
	var (
		lo    = initLogger(opts.Debug)
		index = 0
	)

	// Initialise a db store.
	df, err := datafile.New(opts.Dir, index)
	if err != nil {
		return nil, err
	}

	var flockF *os.File
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

	// Initialise barrel.
	barrel := &Barrel{
		opts:   opts,
		lo:     lo,
		df:     df,
		stale:  make(map[int]*datafile.DataFile, 0),
		flockF: flockF,
		keydir: make(KeyDir, 0),
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}

	// Spawn a goroutine which runs in background and compacts all datafiles in a new single datafile.
	// if opts.MergeInterval == time.Second*0 {
	// 	// TODO: Add a sane default later.
	// 	opts.MergeInterval = time.Second * 5
	// }
	// go barrel.MergeFiles(opts.MergeInterval)
	// // Spawn a goroutine which checks for the file size of the active file at periodic interval.
	// go barrel.ExamineFileSize(time.Minute * 1)

	return barrel, nil
}

// Close closes all the open file descriptors and removes any file locks.
// If non running in a read-only mode, it's essential to call close so that it
// removes any file locks on the database directory. Not calling close will prevent
// future startups until it's removed manually.
func (b *Barrel) Close() {
	// Close all active file handlers.
	b.df.Close()

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

	b.lo.Debug("adding key", "key", k, "val", val)
	return b.put(k, val, nil)
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

	b.lo.Debug("adding key with expiry", "key", k, "val", val, "expiry", ex.String())
	return b.put(k, val, &expiry)
}

// Get takes a key and finds the metadata in the in-memory hashtable (Keydir).
// Using the offset present in metadata it finds the record in the datafile with a single disk seek.
// It further decodes the record and returns the value as a byte array for the given key.
func (b *Barrel) Get(k string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()

	b.lo.Debug("fetching key", "key", k)
	return b.get(k)
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
