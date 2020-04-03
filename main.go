package main

import (
	"reflector-s3-cleaner/configs"
)

func main() {
	err := configs.Init()
	if err != nil {
		panic(err)
	}
}
