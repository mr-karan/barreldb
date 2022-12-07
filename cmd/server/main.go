package main

import (
	"log"
	"os"

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
		Dir:         ".",
		ReadOnly:    false,
		EnableFSync: true,
		MaxFileSize: 1 << 4,
		Debug:       true,
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
	mux.HandleFunc("keys", app.keys)

	err = redcon.ListenAndServe(addr,
		mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
		},
	)
	if err != nil {
		lo.Fatal("error starting server: %w", err)
	}

}
