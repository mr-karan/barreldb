package main

import (
	"log"
	"os"
	"time"

	"github.com/mr-karan/barreldb/pkg/barrel"
	"github.com/tidwall/redcon"
)

var (
	// Version of the build. This is injected at build-time.
	buildString = "unknown"
	lo          = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	addr        = ":6380"
)

type App struct {
	barrel *barrel.Barrel
}

func main() {
	barrel, err := barrel.Init(barrel.Opts{
		Dir:                   ".",
		ReadOnly:              false,
		AlwaysFSync:           true,
		MaxActiveFileSize:     1 << 4,
		SyncInterval:          time.Second * 30,
		Debug:                 false,
		CompactInterval:       time.Second * 20,
		CheckFileSizeInterval: time.Minute * 1,
	})
	if err != nil {
		lo.Fatal("error opening barrel db: %w", err)
	}

	app := &App{
		barrel: barrel,
	}

	mux := redcon.NewServeMux()
	mux.HandleFunc("ping", app.ping)
	mux.HandleFunc("quit", app.quit)
	mux.HandleFunc("set", app.set)
	mux.HandleFunc("get", app.get)
	mux.HandleFunc("del", app.delete)

	if err := redcon.ListenAndServe(addr,
		mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
		},
	); err != nil {
		lo.Fatal("error starting server: %w", err)
	}
}
