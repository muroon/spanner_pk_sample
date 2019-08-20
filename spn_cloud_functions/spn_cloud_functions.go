package spncloudfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"os"
	"time"

	crand "crypto/rand"

	"cloud.google.com/go/spanner"
	"github.com/bwmarrin/snowflake"
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

type TestMode string

const (
	// TestModeSingle single insert
	TestModeSingle TestMode = "single"

	// TestModeBatch batch insert
	TestModeBatch = "batch"

	// TestModeBatchOnly test time only batch execution
	TestModeBatchOnly = "batch_only"
)

// DefaultNumber insert Number
const DefaultNumber int = 10

var modes map[Mode]int

// incrementNum number of auto increment
var incrementNum int64

// client is a global Spanner client, to avoid initializing a new client for
// every request.
var client *spanner.Client

type publishRequest struct {
	Mode     string `json:"mode"`
	TestMode string `json:"testmode"`
	Num      int    `json:"num"`
	Delete   bool   `json:"delete"`
}

// InsertSpanner is an example of querying Spanner from a Cloud Function.
func InsertSpanner(w http.ResponseWriter, r *http.Request) {
	err := executeInsert(w, r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error %#v", err), http.StatusInternalServerError)
		log.Printf("iter.Next: %v", err)
		return
	}
}

func executeInsert(
	w http.ResponseWriter, r *http.Request,
) error {
	ctx := r.Context()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("ioutil.ReadAll: %v", err)
		http.Error(w, "Error reading request", http.StatusBadRequest)
		return nil
	}

	p := publishRequest{}
	if err := json.Unmarshal(data, &p); err != nil {
		log.Printf("json.Unmarshal: %v", err)
		http.Error(w, "Error parsing request", http.StatusBadRequest)
		return nil
	}

	md := Mode(p.Mode)
	tmd := TestMode(p.TestMode)
	num := p.Num
	delete := p.Delete
	fmt.Fprintf(w, "Mode:%v, num:%d\n", md, num)

	// This database must exist.
	projectID := os.Getenv("GCP_PROJECT")
	instanceID := os.Getenv("SPN_INSTANCE_ID")
	databaseID := os.Getenv("SPN_DATABASE_ID")
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		projectID, instanceID, databaseID,
	)

	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Fatalf("Failed to create client %v", err)
	}
	defer client.Close()

	sgr := provideStmtGenerator(md)

	switch tmd {
	case TestModeSingle:
		err = testSingle(ctx, w, r, client, sgr, num)
	case TestModeBatch:
		err = testBatch(ctx, w, r, client, sgr, num)
	case TestModeBatchOnly:
		err = testBatchOnly(ctx, w, r, client, sgr, num)
	}

	if err != nil {
		return err
	}

	if delete {
		_, err = deleteAll(ctx, client)
		if err != nil {
			return err
		}
	}

	return nil
}

func testSingle(
	ctx context.Context, w http.ResponseWriter, r *http.Request, client *spanner.Client, sgr stmtGenerator, num int,
) error {
	startTime := time.Now().UnixNano()

	for i := 0; i < num; i++ {
		incrementNum = int64(i) + 1

		stmt, err := sgr.getStatement(ctx, client, getRandomString(10), getRandomString(10))
		if err != nil {
			return err
		}

		_, err = writeUsingDML(ctx, client, stmt)

		if err != nil {
			return err
		}
	}

	endTime := time.Now().UnixNano()

	fmt.Fprintf(w, "term num:%d nanotime:%d\n", num, endTime-startTime)

	return nil
}

func testBatch(
	ctx context.Context, w http.ResponseWriter, r *http.Request, client *spanner.Client, sgr stmtGenerator, num int,
) error {
	startTime := time.Now().UnixNano()

	stmts := make([]spanner.Statement, 0, num)

	for i := 0; i < num; i++ {
		incrementNum = int64(i) + 1

		stmt, err := sgr.getStatement(ctx, client, getRandomString(10), getRandomString(10))
		if err != nil {
			return err
		}

		stmts = append(stmts, stmt)
	}

	err := writeUsingDMLBatch(ctx, client, stmts)
	if err != nil {
		return err
	}

	endTime := time.Now().UnixNano()

	fmt.Fprintf(w, "term num:%d nanotime:%d\n", num, endTime-startTime)

	return nil
}

func testBatchOnly(
	ctx context.Context, w http.ResponseWriter, r *http.Request, client *spanner.Client, sgr stmtGenerator, num int,
) error {

	stmts := make([]spanner.Statement, 0, num)

	for i := 0; i < num; i++ {
		incrementNum = int64(i) + 1

		stmt, err := sgr.getStatement(ctx, client, getRandomString(10), getRandomString(10))
		if err != nil {
			return err
		}

		stmts = append(stmts, stmt)
	}

	startTime := time.Now().UnixNano()

	err := writeUsingDMLBatch(ctx, client, stmts)
	if err != nil {
		return err
	}

	endTime := time.Now().UnixNano()

	fmt.Fprintf(w, "term num:%d nanotime:%d\n", num, endTime-startTime)

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

func writeUsingDMLBatch(ctx context.Context, client *spanner.Client, stmts []spanner.Statement) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}

		return err
	})
	return err
}

func deleteAll(ctx context.Context, client *spanner.Client) (int64, error) {
	stmt := spanner.Statement{
		SQL: `DELETE FROM Singers WHERE SingerId <> 0`,
	}

	return writeUsingDML(ctx, client, stmt)
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

type stmtGenerator interface {
	getStatement(
		ctx context.Context, client *spanner.Client, firstName, lastName string,
	) (spanner.Statement, error)
}

var stmtGenerators map[Mode]stmtGenerator

func init() {
	stmtGenerators = map[Mode]stmtGenerator{
		ModeFarmFingerPrintConcat: new(stmtGeneratorFarmFingerPrintConcat),
		ModeFarmFingerPrintRand:   new(stmtGeneratorFarmFingerPrintRand),
		ModeRandNum:               new(stmtGeneratorRandNum),
		ModeRandNum2:              new(stmtGeneratorRandNum2),
		ModeTimestampRandNum:      new(stmtGeneratorTimestampRandNum),
		ModeTimestampRandNum2:     new(stmtGeneratorTimestampRandNum2),
		ModeRandNumTimestamp:      new(stmtGeneratorRandNumTimestamp),
	}
}

func provideStmtGenerator(md Mode) stmtGenerator {
	return stmtGenerators[md]
}

type stmtGeneratorFarmFingerPrintConcat struct{}
type stmtGeneratorFarmFingerPrintRand struct{}
type stmtGeneratorRandNum struct{}
type stmtGeneratorRandNum2 struct{}
type stmtGeneratorTimestampRandNum struct{}
type stmtGeneratorTimestampRandNum2 struct{}
type stmtGeneratorRandNumTimestamp struct{}

func (cr stmtGeneratorFarmFingerPrintConcat) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(FARM_FINGERPRINT(CONCAT(@firstName, @lastName)), @firstName, @lastName)`,
		Params: map[string]interface{}{
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorFarmFingerPrintRand) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	key := getRandomString(20)

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(FARM_FINGERPRINT(@key), @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorRandNum) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	key := getRandomInt64(0)

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorRandNum2) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	key := getIncrementNum()
	key = key ^ (key << 13)
	key = key ^ (key >> 7)
	key = key ^ (key << 17)

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorTimestampRandNum) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	tm := time.Now().Unix()
	tm = tm << 32
	num := int64(getRandomUint32())
	key := num + tm

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorTimestampRandNum2) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	stmt := spanner.Statement{}

	// timestampが上位ビットのHashライブラリ
	node, err := snowflake.NewNode(getIncrementNum())
	if err != nil {
		return stmt, err
	}

	id := node.Generate()
	key := id.Int64()

	stmt = spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}

func (cr stmtGeneratorRandNumTimestamp) getStatement(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (spanner.Statement, error) {
	tm := time.Now().Unix()
	num := int64(getRandomUint32()) << 32
	key := num + tm

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return stmt, nil
}
