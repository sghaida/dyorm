package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DBMap define the dynamo db object type
type DBMap map[string]*dynamodb.AttributeValue

// DBModelName the model type
type DBModelName string

// FieldName DynamoDb field name
type FieldName string

// BaseModel define the operations for serialising and deserializing from and to dynamodb type
type BaseModel interface {
	// GetModelType returns the model type eg. order
	GetModelType() DBModelName
	// Marshal marshals the golang object to dynamo map
	Marshal() (DBMap, error)
	// Unmarshal the received dynamo map to a golang object
	Unmarshal(DBMap) (BaseModel, error)
	// GetPartSortKey returns the record's partition and sort key
	GetPartSortKey(name *DynamoTableOrIndexName) DBPSKeyValues
}

// baseModelsWithErr ..
type baseModelsWithErr struct {
	Records []BaseModel
	Err     error
}
