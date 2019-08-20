package spn

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/bwmarrin/snowflake"
)

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
