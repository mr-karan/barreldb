package main

import (
	"fmt"
	"time"

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
	var (
		withExpiry bool
	)
	switch len(cmd.Args) {
	case 4:
		withExpiry = true
	case 3:
		withExpiry = false
	default:
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	var (
		key = string(cmd.Args[1])
		val = cmd.Args[2]
	)
	if withExpiry {
		expiry, err := time.ParseDuration(string(cmd.Args[3]))
		if err != nil {
			conn.WriteError("ERR invalid duration" + string(cmd.Args[3]))
			return
		}
		if err := app.barrel.PutEx(key, val, expiry); err != nil {
			conn.WriteString(fmt.Sprintf("ERR: %s", err))
			return
		}
	} else {
		if err := app.barrel.Put(key, val); err != nil {
			conn.WriteString(fmt.Sprintf("ERR: %s", err))
			return
		}
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
