package spn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"

	crand "crypto/rand"

	"cloud.google.com/go/spanner"
	"github.com/najeira/randstr"
)

// Mode mode type
type Mode string

const (
	// ModeFarmFingerPrintConcat farm_fingerprint mode
	ModeFarmFingerPrintConcat Mode = "farm_fingerprint_concat"

	// ModeFarmFingerPrintRand farm_fingerprint_rand mode
	ModeFarmFingerPrintRand = "farm_fingerprint_random"

	// ModeRandNum random number mode
	ModeRandNum = "random_num"

	// ModeRandNum2 random number by bit inversion
	ModeRandNum2 = "random_num_2"

	// ModeTimestampRandNum timestamp + random number
	ModeTimestampRandNum = "timestamp_random_num"

	// ModeTimestampRandNum2 timestamp + random number (using other package)
	ModeTimestampRandNum2 = "timestamp_random_num_2"

	// ModeRandNumTimestamp random number + timestamp
	ModeRandNumTimestamp = "random_num_timestamp"
)

// DefaultNumber insert Number
const DefaultNumber int = 10

var modes map[Mode]int

// incrementNum number of auto increment
var incrementNum int64

func init() {
	modes = map[Mode]int{
		ModeFarmFingerPrintConcat: 1,
		ModeFarmFingerPrintRand:   1,
		ModeRandNum:               1,
		ModeRandNum2:              1,
		ModeTimestampRandNum:      1,
		ModeTimestampRandNum2:     1,
		ModeRandNumTimestamp:      1,
	}
}

// ExecuteInsert insert execute
func ExecuteInsert(ctx context.Context, md Mode, num int) error {
	if _, ok := modes[md]; !ok {
		return errors.New("invalid parameter.")
	}

	// This database must exist.
	projectID := os.Getenv("SPN_PROJECT_ID")
	instanceID := os.Getenv("SPN_INSTANCE_ID")
	databaseID := os.Getenv("SPN_DATABASE_ID")
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		projectID, instanceID, databaseID,
	)
	fmt.Println(databaseName)

	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Fatalf("Failed to create client %v", err)
	}
	defer client.Close()

	cr := provideCreator(md)

	for i := 0; i < num; i++ {
		incrementNum = int64(i) + 1

		_, err = cr.writeDML(ctx, client, getRandomString(10), getRandomString(10))

		if err != nil {
			return err
		}
	}

	return nil
}

func writeUsingDML(ctx context.Context, client *spanner.Client, stmt spanner.Statement) (int64, error) {
	var rowCount int64
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var err error

		rowCount, err = txn.Update(ctx, stmt)
		if err != nil {
			return err
		}

		return err
	})
	return rowCount, err
}

func getRandomString(num int) string {
	return randstr.CryptoString(num)
}

func getRandomInt64(maxNum int64) int64 {
	seed, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(seed.Int64())
	if maxNum > 0 {
		return rand.Int63() % maxNum
	}
	return rand.Int63()
}

func getRandomUint32() uint32 {
	seed, _ := crand.Int(crand.Reader, big.NewInt(int64(math.MaxUint32)))
	rand.Seed(int64(seed.Uint64()))
	return rand.Uint32()
}

func getIncrementNum() int64 {
	return incrementNum
}
