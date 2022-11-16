package dynamodb_test

import (
	dynamodb "github.com/sghaida/dyorm"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var cfg dynamodb.DBConfig

func TestMain(m *testing.M) {
	pKey := dynamodb.DBKeyName("partKey")
	sKey := dynamodb.DBKeyName("sortKey")
	cfg = dynamodb.DBConfig{
		TableInfo: dynamodb.DBTableInfo{
			TableName: "table",
			DBPSKeyNames: dynamodb.DBPSKeyNames{
				PartitionKey: pKey,
				SortKey:      &sKey,
			},
		},
		Indexes: map[dynamodb.DynamoTableOrIndexName]dynamodb.DBPSKeyNames{
			"index": {
				PartitionKey: pKey,
				SortKey:      &sKey,
			},
		},
	}
	code := m.Run()
	os.Exit(code)
}

func TestNewDynamoDB(t *testing.T) {
	t.Run("successfully", func(t *testing.T) {
		db, _ := dynamodb.NewDynamoDB(cfg)
		assert.NotEmpty(t, db)
	})

	t.Run("missing db table-name", func(t *testing.T) {
		pKey := dynamodb.DBKeyName("partKey")
		sKey := dynamodb.DBKeyName("sortKey")

		cfg := dynamodb.DBConfig{
			Indexes: map[dynamodb.DynamoTableOrIndexName]dynamodb.DBPSKeyNames{
				"default": {
					PartitionKey: pKey,
					SortKey:      &sKey,
				},
			},
		}
		_, err := dynamodb.NewDynamoDB(cfg)
		assert.EqualError(t, err, "invalid db config, missing mandatory keys")
	})

	t.Run("missing table keys", func(t *testing.T) {
		cfg := dynamodb.DBConfig{}

		_, err := dynamodb.NewDynamoDB(cfg)
		assert.EqualError(t, err, "invalid db config, missing mandatory keys")
	})

	t.Run("missing db index's partition key", func(t *testing.T) {
		sKey := dynamodb.DBKeyName("sortKey")
		cfg := dynamodb.DBConfig{
			TableInfo: dynamodb.DBTableInfo{
				TableName: "table",
				DBPSKeyNames: dynamodb.DBPSKeyNames{
					PartitionKey: dynamodb.DBKeyName("pkey"),
				},
			},
			Indexes: map[dynamodb.DynamoTableOrIndexName]dynamodb.DBPSKeyNames{
				"default": {
					SortKey: &sKey,
				},
			},
		}

		_, err := dynamodb.NewDynamoDB(cfg)
		assert.EqualError(t, err, "invalid db config, missing mandatory keys")
	})
}
