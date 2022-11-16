package dynamodb

var (
	pKey = DBKeyName("partKey")
	sKey = DBKeyName("sortKey")
	cfg  = DBConfig{
		TableInfo: DBTableInfo{
			TableName: "table",
			DBPSKeyNames: DBPSKeyNames{
				PartitionKey: pKey,
				SortKey:      &sKey,
			},
		},
	}
)
