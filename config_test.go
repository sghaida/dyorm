package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDbConfig_IsValid(t *testing.T) {
	cases := []struct {
		name     string
		config   DBConfig
		expected bool
	}{
		{
			name: "successfully with table config only",
			config: DBConfig{
				TableInfo: DBTableInfo{
					TableName: "table",
					DBPSKeyNames: DBPSKeyNames{
						PartitionKey: pKey,
						SortKey:      &sKey,
					},
				},
			},
			expected: true,
		},
		{
			name: "successfully with indexes config",
			config: DBConfig{
				TableInfo: DBTableInfo{
					TableName: "table",
					DBPSKeyNames: DBPSKeyNames{
						PartitionKey: pKey,
						SortKey:      &sKey,
					},
				},
				Indexes: map[DynamoTableOrIndexName]DBPSKeyNames{
					"index": {
						PartitionKey: pKey,
						SortKey:      &sKey,
					},
				},
			},
			expected: true,
		},
		{
			name: "with missing partition key (table info)",
			config: DBConfig{
				TableInfo: DBTableInfo{
					TableName: "table",
					DBPSKeyNames: DBPSKeyNames{
						SortKey: &sKey,
					},
				},
			},
		},
		{
			name: "with missing partition key",
			config: DBConfig{
				TableInfo: DBTableInfo{
					TableName: "table",
					DBPSKeyNames: DBPSKeyNames{
						PartitionKey: pKey,
					},
				},
				Indexes: map[DynamoTableOrIndexName]DBPSKeyNames{
					"index": {
						SortKey: &sKey,
					},
				},
			},
		},
		{
			name: "with missing table name",
			config: DBConfig{
				TableInfo: DBTableInfo{
					DBPSKeyNames: DBPSKeyNames{
						PartitionKey: pKey,
						SortKey:      &sKey,
					},
				},
				Indexes: map[DynamoTableOrIndexName]DBPSKeyNames{
					"index": {
						PartitionKey: pKey,
						SortKey:      &sKey,
					},
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.config.IsValid()
			assert.Equal(t, tc.expected, actual)
		})
	}
}
