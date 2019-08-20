package main

import (
	"context"
	"flag"
	"fmt"
	"spanner_pk_sample/spn"

	"github.com/apex/log"
)

var mode string
var testMode string
var num int
var delete bool

func init() {
	const usage = "the variety of mode"

	flag.StringVar(&mode, "mode", string(spn.ModeFarmFingerPrintConcat), usage)
	flag.StringVar(&testMode, "testmode", string(spn.TestModeSingle), usage)
	flag.IntVar(&num, "num", spn.DefaultNumber, usage)
	flag.BoolVar(&delete, "post-delete", false, usage)
}

func main() {
	flag.Parse()

	ctx := context.Background()

	fmt.Printf("mode:%s, testMode:%s\n", mode, testMode)

	if err := spn.ExecuteInsert(
		ctx, spn.Mode(mode), spn.TestMode(testMode), num, delete,
	); err != nil {
		log.Errorf("error is occuer. %#v\n", err)
	}
}
