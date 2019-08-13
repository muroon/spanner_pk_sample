package main

import (
	"context"
	"flag"
	"fmt"
	"spanner_pk_sample/spn"

	"github.com/apex/log"
)

var mode string
var num int

func init() {
	const usage = "the variety of mode"

	flag.StringVar(&mode, "mode", string(spn.ModeFarmFingerPrintConcat), usage)
	flag.IntVar(&num, "num", spn.DefaultNumber, usage)
}

func main() {
	flag.Parse()

	ctx := context.Background()

	fmt.Printf("mode:%s\n", mode)

	if err := spn.ExecuteInsert(
		ctx, spn.Mode(mode), num,
	); err != nil {
		log.Errorf("error is occuer. %#v\n", err)
	}
}
