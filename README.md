# DyORM
[![codecov](https://codecov.io/gh/sghaida/dyorm/branch/main/graph/badge.svg?token=L7ATQJC1CC)](https://codecov.io/gh/sghaida/dyorm)
[![Code Grade](https://api.codiga.io/project/35012/status/svg)](https://app.codiga.io/hub/project/35012/dyorm)
[![workflow](https://github.com/sghaida/dyorm/actions/workflows/ci.yml/badge.svg)](https://github.com/sghaida/dyorm/actions/workflows/ci.yml/badge.svg)

small and opinionated DynamoDB ORM, which quite easy to kick you start with DynamoDB calls. 
this Lib implementing the following calls 

- Query Operations

```go
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
```
- Command Operations

```go
// DBCommands DynamoDB commands related interface
type DBCommands interface {
	// AddRecord inserts a new record to dynamo DB table
	AddRecord(ctx context.Context, in BaseModel, createSortKey bool) (DBPSKeyValues, error)
	// UpdateRecordByID updates a dynamodb record
	UpdateRecordByID(ctx context.Context, in BaseModel, dbKeys DBPSKeyValues) error
	// DeleteRecordByID deletes a dynamodb record if the passed filters were matched:
	DeleteRecordByID(ctx context.Context, dbKeys DBPSKeyValues, filters *AwsExpressionWrapper) error
}
```

- Batch or Bulk Operations
```go
// DBBulkCommands Dynamo Bulk commands related interface
type DBBulkCommands interface {
	// BulkAddRecords inserts a bulk of records (maximum 25 item at a time) into dynamodb table
	BulkAddRecords(ctx context.Context, baseModel BaseModel, createSortKey bool, records ...BaseModel) ([]BaseModel, error)
	// BulkUpdateRecords updates multiple dynamo records
	BulkUpdateRecords(ctx context.Context, baseModel BaseModel, records ...BaseModel) ([]BaseModel, error)
	// BulkDeleteRecords delete a bulk of dynamo records
	BulkDeleteRecords(ctx context.Context, dbKeys ...DBPSKeyValues) ([]DBPSKeyValues, error)
}
```

## How to use 

- define your model that is supposed to be mapped to DynamoDB table.
Notes to consider, the json annotations will reflect the columns names
```go
type User struct {
    ID           string `json:"user_id,omitempty"`
    FirstName    string `json:"first_name,omitempty"`
    LastName     string `json:"last_name,omitempty"`
    EmailAddress string `json:"email_address,omitempty"`
}
```
- implement the following functionalities that is provided in [Base Model Interface](./base_model.go)
```go
// GetModelType return the name of the table
func (user User) GetModelType() DBModelName {
    return "user"
}
// Marshal convert User Model to Dynamo Map
func (user User) Marshal() (DBMap, error) {
    return dynamodbattribute.MarshalMap(user)
}

// Unmarshal convert DynamoDB Map to Base model
func (user User) Unmarshal(dbMap DBMap) (BaseModel, error) {
    usr := User{}
    err := dynamodbattribute.UnmarshalMap(dbMap, &usr)
    return usr, err
}

// GetPartSortKey returns the values for the Partition and sorting key for the table or table index
func (user User) GetPartSortKey(index *DynamoTableOrIndexName) DBPSKeyValues {
    // creates the table main primary key
    tableSortKey := DBKeyValue(user.EmailAddress)
    mainPSKeys := NewDbPSKeyValues(DBKeyValue(user.ID), &tableSortKey)
    if index == nil {
        return mainPSKeys
    }
    // define the table indexes key's values
    keysByIndexes := map[DynamoTableOrIndexName]DBPSKeyValues{
        // user_by_email index has only partition key
        "user_by_email_index": NewDbPSKeyValues(DBKeyValue(user.EmailAddress), nil),
    }
    return keysByIndexes[*index]
}

```
- create the repo 
```go
// repo repository interface
type repo interface {
    createUser(ctx context.Context, req User) error
    updateUser(ctx context.Context, req User) error
    getUserByID(ctx context.Context, id string) (User, error)
    getAll(ctx context.Context, pageSize int, lastItemID string, ch chan<- User)
}
//repo implementation
type dbHandler struct {
    db     DBHandler
    config DBConfig
}


func newUserRepo(db DBHandler, config DBConfig) repo {
    return &dbHandler{db: db, config: config}
}

func (h dbHandler) createUser(ctx context.Context, req User) error {
    _, err := h.db.AddRecord(ctx, req, true)
    if err != nil {
        return errors.Wrap(err, "failed to create user")
    }
    return nil
}

func (h dbHandler) updateUser(ctx context.Context, req User) error {
    // you can update by using two different dynamodb calls
    // 1. Update(ctx context.Context, partKey string, sortKey *string, data map[FieldName]interface{})
    // you could use this for partial and full update
    // 2. UpdateRecordByID(ctx context.Context, in BaseModel, dbKeys DBPSKeyValues)
    _, err := h.db.AddRecord(ctx, req, false)
    return err
}

func (h dbHandler) getUserByID(ctx context.Context, id string) (User, error) {
    pKey := NewDbPSKeyValues(DBKeyValue(id), nil)
    res, err := h.db.GetByID(ctx, User{}, DynamoTableOrIndexName(h.config.TableInfo.TableName), pKey)
    if err != nil {
        return User{}, err
    }
    return res.(User), nil
}

func (h dbHandler) getAll(ctx context.Context, pageSize int, lastItemID string, ch chan<- User) {
    // please note that this is a scan query which is quite expensive in terms of cost and time
    // you can use filter operation to reduce the result space but the operation will execute on
    // dynamodb then the filter wil be applied
    // the results will be paginated. please refer to aws limits for scan queries
    // use it with caution
    defer close(ch)

    filter := NewExpressionWrapper(h.config.TableInfo.TableName).
        WithKeyCondition("first_name", "saddam", EQUAL).
        AndCondition("last_name", "abu", EQUAL).
        WithLimit(int64(pageSize))
    if lastItemID != "" {
        filter.WithLastEvaluatedKey("user_id", lastItemID, nil, nil)
    }
    items, err := h.db.GetRecordsWithScanFilter(ctx, User{}, filter)
    if err != nil {
        // you should handle the error, as for what is written to show the usage
        ch <- User{}
        return
    }

    for _, item := range items {
        user, ok := item.(User)
        if !ok {
            // you should handle the error, as for what is written to show the usage
            ch <- User{}
            return
        }
        // everything is fine
        ch <- user
    }
}
func (h dbHandler) getAllByScan(ctx context.Context, pageSize int, lastItemID string, ch chan<- User) {
    // please note that this is a query which is efficient in terms of cost and time
    // you can use filter operation on indexed fields to reduce the data size
    // the results will be paginated. please refer to aws limits for scan queries
    // use it with caution
    defer close(ch)

    filter := NewExpressionWrapper(h.config.TableInfo.TableName).
        WithKeyCondition("first_name", "saddam", EQUAL).
        AndCondition("last_name", "abu", EQUAL).
        WithLimit(int64(pageSize))
    if lastItemID != "" {
        filter.WithLastEvaluatedKey("user_id", lastItemID, nil, nil)
    }
    items, err := h.db.GetRecordsWithQueryFilter(ctx, User{}, filter)
    if err != nil {
        // you should handle the error, as for what is written to show the usage
        ch <- User{}
        return
    }

    for _, item := range items {
        user, ok := item.(User)
        if !ok {
            // you should handle the error, as for what is written to show the usage
            ch <- User{}
            return
        }
        // everything is fine
        ch <- user
    }
}
```
