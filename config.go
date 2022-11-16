package dynamodb

// DynamoTableOrIndexName define the dynamo table index ( LSI or GSI)
type DynamoTableOrIndexName string

// DBKeyName a type for dynamo partition key or sorting key
type DBKeyName string

// DBPSKeyNames hold the attribute name(s) for a table or a table index' s partition and sort keys
type DBPSKeyNames struct {
	PartitionKey DBKeyName
	SortKey      *DBKeyName
}

// DBTableInfo holds the TableName, Partition key and sorting key if available
type DBTableInfo struct {
	TableName string
	DBPSKeyNames
}

// DBConfig define the database config type
// hold the main table name, partition key, and sorting key if available
// along with all the info for the indices keyed by the Index name
// and the value for the indices map is the partition key, sorting key if available
type DBConfig struct {
	TableInfo DBTableInfo
	Indexes   map[DynamoTableOrIndexName]DBPSKeyNames
}

// IsValid check if the configuration is valid
func (c DBConfig) IsValid() bool {
	if len(c.TableInfo.TableName) < 1 || len(c.TableInfo.PartitionKey) < 1 {
		return false
	}
	for _, dbIndex := range c.Indexes {
		if len(dbIndex.PartitionKey) < 1 {
			return false
		}
	}
	return true
}
