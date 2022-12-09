package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mr-karan/barreldb/pkg/barrel"
	"github.com/tidwall/redcon"
	"github.com/zerodha/logf"
)

var (
	// Version of the build. This is injected at build-time.
	buildString = "unknown"
)

type App struct {
	lo     logf.Logger
	barrel *barrel.Barrel
}

func main() {
	// Initialise and load the config.
	ko, err := initConfig()
	if err != nil {
		fmt.Printf("error loading config: %v", err)
		os.Exit(-1)
	}

	app := &App{
		lo: initLogger(ko),
	}
	app.lo.Info("booting barreldb server", "version", buildString)

	barrel, err := barrel.Init(barrel.Opts{
		Debug:                 ko.Bool("app.debug"),
		Dir:                   ko.MustString("app.dir"),
		ReadOnly:              ko.Bool("app.read_only"),
		AlwaysFSync:           ko.Bool("app.always_fsync"),
		SyncInterval:          ko.Duration("app.fsync_interval"),
		MaxActiveFileSize:     ko.Int64("app.max_file_size"),
		CompactInterval:       ko.Duration("app.compaction_interval"),
		CheckFileSizeInterval: ko.Duration("app.eval_file_size_interval"),
	})
	if err != nil {
		app.lo.Fatal("error opening barrel db", "error", err)
	}

	app.barrel = barrel

	// Initialise server.
	mux := redcon.NewServeMux()
	mux.HandleFunc("ping", app.ping)
	mux.HandleFunc("quit", app.quit)
	mux.HandleFunc("set", app.set)
	mux.HandleFunc("get", app.get)
	mux.HandleFunc("del", app.delete)

	// Create a channel to listen for cancellation signals.
	// Create a new context which is cancelled when `SIGINT`/`SIGTERM` is received.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	srvr := redcon.NewServer(ko.MustString("server.address"),
		mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
		},
	)

	// Sart the server in a goroutine.
	go func() {
		if err := srvr.ListenAndServe(); err != nil {
			app.lo.Fatal("failed to listen and serve", "error", err)
		}
	}()

	// Listen on the close channel indefinitely until a
	// `SIGINT` or `SIGTERM` is received.
	<-ctx.Done()

	// Cancel the context to gracefully shutdown and perform
	// any cleanup tasks.
	cancel()
	app.barrel.Shutdown()
	srvr.Close()
}
