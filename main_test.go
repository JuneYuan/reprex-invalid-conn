package main

import (
	"log"
	"testing"
	"time"
)

func init() {
	Init()
}

func Test_scanTableA(t *testing.T) {
	go scanTableA()

	go func() {
		for r := range recordsCh {
			log.Printf("%+v\n", r)
		}
	}()

	time.Sleep(10 * time.Second)
}
