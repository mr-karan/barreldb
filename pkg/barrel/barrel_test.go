package barrel

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInitDefaults(t *testing.T) {
	var (
		brl    = &Barrel{}
		assert = assert.New(t)
	)

	// Create a temp directory for running tests.
	tmpDir, err := os.MkdirTemp("", "barreldb")
	defer os.RemoveAll(tmpDir)

	assert.NoError(err)

	t.Run("Init_Defaults", func(t *testing.T) {
		brl, err = Init(WithDir(tmpDir))
		assert.NoError(err)
		assert.NotEmpty(brl)

		// Check config.
		assert.Equal(brl.opts.dir, tmpDir)

		// Check defaults.
		assert.Equal(false, brl.opts.debug, "debug is wrongly set")
		assert.Equal(false, brl.opts.readOnly, "readOnly is wrongly set")
		assert.Equal(false, brl.opts.alwaysFSync, "alwaysFSync is wrongly set")
		assert.Equal(defaultMaxActiveFileSize, brl.opts.maxActiveFileSize, "defaultMaxActiveFileSize is wrongly set")
		assert.Equal(defaultCompactInterval, brl.opts.compactInterval, "defaultCompactInterval is wrongly set")
		assert.Equal(defaultFileSizeInterval, brl.opts.checkFileSizeInterval, "defaultFileSizeInterval is wrongly set")
		assert.Nil(brl.opts.syncInterval, "syncInterval is wrongly set")
	})

	t.Run("Close", func(t *testing.T) {
		err = brl.Shutdown()
		assert.NoError(err)
	})
}

func TestInitWithOpts(t *testing.T) {
	var (
		brl    = &Barrel{}
		assert = assert.New(t)
	)

	// Create a temp directory for running tests.
	tmpDir, err := os.MkdirTemp("", "barreldb")
	defer os.RemoveAll(tmpDir)

	assert.NoError(err)

	t.Run("Init_Custom", func(t *testing.T) {
		brl, err = Init(WithDir(tmpDir), WithAlwaysSync(), WithDebug(), WithMaxActiveFileSize(int64(1<<4)), WithCheckFileSizeInterval(time.Second*15))
		assert.NoError(err)
		assert.NotEmpty(brl)

		// Check config.
		assert.Equal(true, brl.opts.alwaysFSync)
		assert.Equal(true, brl.opts.debug)
		assert.Equal(int64(1<<4), brl.opts.maxActiveFileSize)
		assert.Equal(time.Second*15, brl.opts.checkFileSizeInterval)
	})

	t.Run("Close", func(t *testing.T) {
		err = brl.Shutdown()
		assert.NoError(err)
	})
}

func TestAPI(t *testing.T) {
	var (
		brl    = &Barrel{}
		assert = assert.New(t)
	)

	// Create a temp directory for running tests.
	tmpDir, err := os.MkdirTemp("", "barreldb")
	defer os.RemoveAll(tmpDir)

	assert.NoError(err)

	t.Run("Init", func(t *testing.T) {
		brl, err = Init(WithDir(tmpDir))
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		err = brl.Put("hello", []byte("world"))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		val, err := brl.Get("hello")
		assert.NoError(err)
		assert.Equal("world", string(val), "value is not equal")
	})

	t.Run("List", func(t *testing.T) {
		keys := brl.List()
		assert.NotEmpty(keys)
		assert.Len(keys, 1)
		assert.Equal([]string{"hello"}, keys)
	})

	t.Run("Len", func(t *testing.T) {
		len := brl.Len()
		assert.Equal(len, 1)
	})

	t.Run("Fold", func(t *testing.T) {
		upper := func(s string) error {
			fmt.Println(strings.ToUpper(s))
			return nil
		}
		err = brl.Fold(upper)
		assert.NoError(err)
	})

	t.Run("Expiry", func(t *testing.T) {
		err = brl.PutEx("keywithexpiry", []byte("valwithex"), time.Second*2)
		assert.NoError(err)
		time.Sleep(time.Second * 3)

		_, err := brl.Get("keywithexpiry")
		assert.Error(err)
		assert.ErrorIs(err, ErrExpiredKey)
	})

	t.Run("Delete", func(t *testing.T) {
		err = brl.Delete("hello")
		assert.NoError(err)
		_, err = brl.Get("hello")
		assert.Error(err)
		assert.ErrorIs(err, ErrNoKey)
	})

	t.Run("Sync", func(t *testing.T) {
		err = brl.Sync()
		assert.NoError(err)
	})

	t.Run("Close", func(t *testing.T) {
		err = brl.Shutdown()
		assert.NoError(err)
	})
}
