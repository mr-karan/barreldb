package barrel_test

import (
	"os"
	"strings"
	"testing"

	barrel "github.com/mr-karan/barreldb"
)

func BenchmarkPut(b *testing.B) {
	// Create a temp directory for running tests.
	tmpDir, err := os.MkdirTemp("", "barreldb")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	scenarios := map[string][]barrel.Config{
		"AlwaysSync":  {barrel.WithDir(tmpDir), barrel.WithAlwaysSync()},
		"DisableSync": {barrel.WithDir(tmpDir)},
	}

	for sc, cfg := range scenarios {
		brl, err := barrel.Init(cfg...)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(sc, func(b *testing.B) {
			// Size of each value -> 4kb.
			b.SetBytes(int64(4096))
			b.ReportAllocs()

			var (
				key = "hello"
				val = []byte(strings.Repeat(" ", 4096))
			)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				err = brl.Put(key, val)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
		brl.Shutdown()
	}
}

func BenchmarkGet(b *testing.B) {
	// Create a temp directory for running tests.
	tmpDir, err := os.MkdirTemp("", "barreldb")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	brl, err := barrel.Init(barrel.WithDir(tmpDir))
	if err != nil {
		b.Fatal(err)
	}
	defer brl.Shutdown()

	// Size of each value -> 4kb.
	b.SetBytes(int64(4096))
	b.ReportAllocs()

	var (
		key = "hello"
		val = []byte(strings.Repeat(" ", 4096))
	)

	// Put dummy keys.
	for i := 0; i < b.N; i++ {
		err = brl.Put(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := brl.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
