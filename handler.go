package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
)

// DBKeyValue a type for partition or sort key
type DBKeyValue string

// DBPSKeyValues holds the partition and the sort keys
type DBPSKeyValues interface {
	GetPartitionKey() DBKeyValue
	GetSortKey() *DBKeyValue
}

// DBAttributeValues : type to return LastEvaluatedKey
type DBAttributeValues map[string]*dynamodb.AttributeValue

type dbPSKeyValues struct {
	partitionKey DBKeyValue
	sortKey      *DBKeyValue
}

// NewDbPSKeyValues factory to create a new primary key
func NewDbPSKeyValues(partitionKey DBKeyValue, sortKey *DBKeyValue) DBPSKeyValues {
	return dbPSKeyValues{
		partitionKey: partitionKey,
		sortKey:      sortKey,
	}
}

// GetPartitionKey returns the partition key's value for a table or an index
func (d dbPSKeyValues) GetPartitionKey() DBKeyValue {
	return d.partitionKey
}

// GetSortKey returns the sort key's value for a table or an index
func (d dbPSKeyValues) GetSortKey() *DBKeyValue {
	return d.sortKey
}

// DBQueries DynamoDB query related interface
type DBQueries interface {
	// GetByID get by partition (& sort) key(s)
	GetByID(ctx context.Context, input BaseModel, name DynamoTableOrIndexName, dbKeys DBPSKeyValues) (BaseModel, error)
	// GetByIDs get records by their partition (& sort) keys
	GetByIDs(ctx context.Context, input BaseModel, dbKeys []DBPSKeyValues) ([]BaseModel, error)
	// GetRecordsWithScanFilter gets all records that match the provided filter using scan req
	// @TODO change it to map[string]interface{}
	GetRecordsWithScanFilter(ctx context.Context, input BaseModel, filters *AwsExpressionWrapper) ([]BaseModel, DBAttributeValues, error)
	// GetRecordsWithQueryFilter gets all records that match the provided filter using query req
	GetRecordsWithQueryFilter(ctx context.Context, input BaseModel, filters *AwsExpressionWrapper) ([]BaseModel, DBAttributeValues, error)
}

// DBCommands DynamoDB commands related interface
type DBCommands interface {
	// AddRecord inserts a new record to dynamo DB table
	AddRecord(ctx context.Context, in BaseModel, createSortKey bool) (DBPSKeyValues, error)
	// UpdateRecordByID updates a dynamodb record
	UpdateRecordByID(ctx context.Context, in BaseModel, dbKeys DBPSKeyValues) error
	// DeleteRecordByID deletes a dynamodb record if the passed filters were matched:
	DeleteRecordByID(ctx context.Context, dbKeys DBPSKeyValues, filters *AwsExpressionWrapper) error
}

// DBBulkCommands Dynamo Bulk commands related interface
type DBBulkCommands interface {
	// BulkAddRecords inserts a bulk of records (maximum 25 item at a time) into dynamodb table
	BulkAddRecords(ctx context.Context, baseModel BaseModel, createSortKey bool, records ...BaseModel) ([]BaseModel, error)
	// BulkUpdateRecords updates multiple dynamo records
	BulkUpdateRecords(ctx context.Context, baseModel BaseModel, records ...BaseModel) ([]BaseModel, error)
	// BulkDeleteRecords delete a bulk of dynamo records
	BulkDeleteRecords(ctx context.Context, dbKeys ...DBPSKeyValues) ([]DBPSKeyValues, error)
}

// DBHandler DynamoDB interface
type DBHandler interface {
	DBQueries
	DBCommands
	DBBulkCommands
}

type handlerImp struct {
	config DBConfig
	dynamodbiface.DynamoDBAPI
}

// NewDynamoDB returns a dynamo DB handler
// take as argument the table config: table name and its indexes keys
func NewDynamoDB(cfg DBConfig) (DBHandler, error) {
	// validate the config
	if !cfg.IsValid() {
		return nil, errors.New("invalid db config, missing mandatory keys")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	client := dynamodb.New(sess)
	return &handlerImp{config: cfg, DynamoDBAPI: client}, nil
}
