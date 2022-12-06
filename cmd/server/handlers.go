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
