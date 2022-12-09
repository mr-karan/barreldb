package main

import (
	"fmt"
	"os"
	"strings"

	flag "github.com/spf13/pflag"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/zerodha/logf"
)

// initLogger initializes logger instance.
func initLogger(ko *koanf.Koanf) logf.Logger {
	opts := logf.Opts{EnableCaller: true}
	if ko.String("app.log") == "debug" {
		opts.Level = logf.DebugLevel
		opts.EnableColor = true
	}
	return logf.New(opts)
}

// initConfig loads config to `ko` object.
func initConfig() (*koanf.Koanf, error) {
	var (
		ko = koanf.New(".")
		f  = flag.NewFlagSet("front", flag.ContinueOnError)
	)

	// Configure Flags.
	f.Usage = func() {
		fmt.Println(f.FlagUsages())
		os.Exit(0)
	}

	// Register `--config` flag.
	cfgPath := f.String("config", "config.sample.toml", "Path to a config file to load.")

	// Parse and Load Flags.
	err := f.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	err = ko.Load(file.Provider(*cfgPath), toml.Parser())
	if err != nil {
		return nil, err
	}
	err = ko.Load(env.Provider("BARRELDB_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "BARRELDB_")), "__", ".", -1)
	}), nil)
	if err != nil {
		return nil, err
	}
	return ko, nil
}
