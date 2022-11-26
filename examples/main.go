package main

import (
	"fmt"

	"github.com/mr-karan/barreldb/pkg/barrel"
)

func main() {
	barrel, err := barrel.Init(".", barrel.Opts{
		ReadOnly:    false,
		EnableFSync: true,
	})
	if err != nil {
		panic(err)
	}

	if err := barrel.Put("hello", []byte("world")); err != nil {
		panic(err)
	}
	if err := barrel.Put("good", []byte("bye")); err != nil {
		panic(err)
	}

	val, err := barrel.Get("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	val, err = barrel.Get("good")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

	keys := barrel.List()
	fmt.Println(keys)

	val, err = barrel.Get("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	barrel.Close()
}
