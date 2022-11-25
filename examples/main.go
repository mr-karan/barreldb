package main

import (
	"fmt"

	"github.com/mr-karan/barreldb/pkg/barrel"
)

func main() {
	barrel, err := barrel.Init(".")
	if err != nil {
		panic(err)
	}

	if err := barrel.Put("hello", []byte("world!")); err != nil {
		panic(err)
	}
	// if err := barrel.Put("hello", []byte("world!")); err != nil {
	// 	panic(err)
	// }

	val, err := barrel.Get("hello")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

}
