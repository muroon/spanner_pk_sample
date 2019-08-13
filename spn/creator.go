package spn

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/bwmarrin/snowflake"
)

type creator interface {
	writeDML(
		ctx context.Context, client *spanner.Client, firstName, lastName string,
	) (int64, error)
}

var creators map[Mode]creator

func init() {
	creators = map[Mode]creator{
		ModeFarmFingerPrintConcat: new(creatorFarmFingerPrintConcat),
		ModeFarmFingerPrintRand:   new(creatorFarmFingerPrintRand),
		ModeRandNum:               new(creatorRandNum),
		ModeRandNum2:              new(creatorRandNum2),
		ModeTimestampRandNum:      new(creatorTimestampRandNum),
		ModeTimestampRandNum2:     new(creatorTimestampRandNum2),
		ModeRandNumTimestamp:      new(creatorRandNumTimestamp),
	}
}

func provideCreator(md Mode) creator {
	return creators[md]
}

type creatorFarmFingerPrintConcat struct{}
type creatorFarmFingerPrintRand struct{}
type creatorRandNum struct{}
type creatorRandNum2 struct{}
type creatorTimestampRandNum struct{}
type creatorTimestampRandNum2 struct{}
type creatorRandNumTimestamp struct{}

func (cr creatorFarmFingerPrintConcat) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(FARM_FINGERPRINT(CONCAT(@firstName, @lastName)), @firstName, @lastName)`,
		Params: map[string]interface{}{
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorFarmFingerPrintRand) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
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

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorRandNum) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
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

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorRandNum2) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
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

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorTimestampRandNum) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
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

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorTimestampRandNum2) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
	// timestampが上位ビットのHashライブラリ
	node, err := snowflake.NewNode(getIncrementNum())
	if err != nil {
		return 0, err
	}

	id := node.Generate()
	key := id.Int64()

	stmt := spanner.Statement{
		SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES 
					(@key, @firstName, @lastName)`,
		Params: map[string]interface{}{
			"key":       key,
			"firstName": firstName,
			"lastName":  lastName,
		},
	}

	return writeUsingDML(ctx, client, stmt)
}

func (cr creatorRandNumTimestamp) writeDML(
	ctx context.Context, client *spanner.Client, firstName, lastName string,
) (int64, error) {
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

	return writeUsingDML(ctx, client, stmt)
}
