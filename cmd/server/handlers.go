package main

import (
	"fmt"

	"github.com/tidwall/redcon"
)

func (app *App) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (app *App) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (app *App) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var (
		key = string(cmd.Args[1])
		val = cmd.Args[2]
	)
	if err := app.barrel.Put(key, val); err != nil {
		conn.WriteString(fmt.Sprintf("ERR: %s", err))
		return
	}

	conn.WriteString("OK")
}

func (app *App) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var (
		key = string(cmd.Args[1])
	)
	val, err := app.barrel.Get(key)
	if err != nil {
		conn.WriteString(fmt.Sprintf("ERR: %s", err))
		return
	}

	conn.WriteBulk(val)
}

func (app *App) delete(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var (
		key = string(cmd.Args[1])
	)
	err := app.barrel.Delete(key)
	if err != nil {
		conn.WriteString(fmt.Sprintf("ERR: %s", err))
		return
	}

	conn.WriteNull()
}

func (app *App) keys(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var (
		key = string(cmd.Args[1])
	)

	// Only supports listing all keys for now.
	if key != "*" {
		conn.WriteError("ERR: Only * is supported as a pattern for now'")
		return
	}

	keys := app.barrel.List()

	if len(keys) == 0 {
		conn.WriteArray(0)
		return
	}

	for _, k := range keys {
		conn.WriteBulkString(k)
	}

}
