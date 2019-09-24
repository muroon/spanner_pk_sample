package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"spanner_pk_sample/spn"

	"github.com/apex/log"
)

var mode string
var testMode string
var num int
var delete bool
var spnm spn.ISpannerManager

func init() {
	const usage = "the variety of mode"

	flag.StringVar(&mode, "mode", string(spn.ModeFarmFingerPrintConcat), usage)
	flag.StringVar(&testMode, "testmode", string(spn.TestModeSingle), usage)
	flag.IntVar(&num, "num", spn.DefaultNumber, usage)
	flag.BoolVar(&delete, "post-delete", false, usage)

	spnm = spn.NewSpannerManager(
		spn.SetProjectID(os.Getenv("SPN_PROJECT_ID")),
		spn.SetInstanceID(os.Getenv("SPN_INSTANCE_ID")),
		spn.SetDatabaseID(os.Getenv("SPN_DATABASE_ID")),
	)
}

func main() {
	flag.Parse()

	ctx := context.Background()

	fmt.Printf("mode:%s, testMode:%s\n", mode, testMode)

	if err := spnm.ExecuteInsert(
		ctx, spn.Mode(mode), spn.TestMode(testMode), num, delete,
	); err != nil {
		log.Errorf("error is occuer. %#v\n", err)
	}
}
