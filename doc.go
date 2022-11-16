// Package dynamodb ...
// implements the following functionalities
// query: GetByID, GetByIDs, GetRecordsWithScanFilter, GetRecordsWithQueryFilter
// command: AddRecord, UpdateRecordByID, DeleteRecordByID
// bulk operations: BulkAddRecords, BulkUpdateRecords, BulkDeleteRecords
// for bulk operations and get all there is some AWS dynamo limits regarding the number of records and size
// please refer to aws documentation
//
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
//
// DB model implement BaseModel interface
// json tags to be able to translate from and to DB record
// where the json field name is the name of the record in DynamoDB
//	type User struct {
//		ID           string `json:"user_id,omitempty"`
//		FirstName    string `json:"first_name,omitempty"`
//		LastName     string `json:"last_name,omitempty"`
//		EmailAddress string `json:"email_address,omitempty"`
//	}
//
//	func (user User) GetModelType() DBModelName {
//		return "user"
//	}
//
//	func (user User) Marshal() (DBMap, error) {
//		return dynamodbattribute.MarshalMap(user)
//	}
//
//	func (user User) Unmarshal(dbMap DBMap) (BaseModel, error) {
//		usr := User{}
//		err := dynamodbattribute.UnmarshalMap(dbMap, &usr)
//		return usr, err
//	}
//
//	func (user User) GetPartSortKey(index *DynamoTableOrIndexName) DBPSKeyValues {
//		// creates the table main primary key
//		tableSortKey := DBKeyValue(user.EmailAddress)
//		mainPSKeys := NewDbPSKeyValues(DBKeyValue(user.ID), &tableSortKey)
//		if index == nil {
//			return mainPSKeys
//		}
//		// define the table indexes key's values
//		keysByIndexes := map[DynamoTableOrIndexName]DBPSKeyValues{
//			// user_by_email index has only partition key
//			"user_by_email_index": NewDbPSKeyValues(DBKeyValue(user.EmailAddress), nil),
//		}
//		return keysByIndexes[*index]
//	}
//
// repo repository interface
//	type repo interface {
//		createUser(ctx context.Context, req User) error
//		updateUser(ctx context.Context, req User) error
//		getUserByID(ctx context.Context, id string) (User, error)
//		getAll(ctx context.Context, pageSize int, lastItemID string, ch chan<- User)
//	}
//
//repo implementation
//	type dbHandler struct {
//		db     DBHandler
//		config DBConfig
//	}
//
//
//	func newUserRepo(db DBHandler, config DBConfig) repo {
//		return &dbHandler{db: db, config: config}
//	}
//
//	func (h dbHandler) createUser(ctx context.Context, req User) error {
//		_, err := h.db.AddRecord(ctx, req, true)
//		if err != nil {
//			return errors.Wrap(err, "failed to create user")
//		}
//
//		return nil
//	}
//
//	func (h dbHandler) updateUser(ctx context.Context, req User) error {
//		// you can update by using two different dynamodb calls
//		// 1. Update(ctx context.Context, partKey string, sortKey *string, data map[FieldName]interface{})
//		// you could use this for partial and full update
//		// 2. UpdateRecordByID(ctx context.Context, in BaseModel, dbKeys DBPSKeyValues)
//		_, err := h.db.AddRecord(ctx, req, false)
//		return err
//	}
//
//	func (h dbHandler) getUserByID(ctx context.Context, id string) (User, error) {
//		pKey := NewDbPSKeyValues(DBKeyValue(id), nil)
//		res, err := h.db.GetByID(ctx, User{}, DynamoTableOrIndexName(h.config.TableInfo.TableName), pKey)
//		if err != nil {
//			return User{}, err
//		}
//		return res.(User), nil
//	}
//
//	func (h dbHandler) getAll(ctx context.Context, pageSize int, lastItemID string, ch chan<- User) {
//		// please note that this is a scan query which is quite expensive in terms of cost and time
//		// you can use filter operation to reduce the result space but the operation will execute on
//		// dynamodb then the filter wil be applied
//		// the results will be paginated. please refer to aws limits for scan queries
//		// use it with caution
//		defer close(ch)
//
//		filter := NewExpressionWrapper(h.config.TableInfo.TableName).
//			WithKeyCondition("first_name", "saddam", EQUAL).
//			AndCondition("last_name", "abu", EQUAL).
//			WithLimit(int64(pageSize))
//		if lastItemID != "" {
//			filter.WithLastEvaluatedKey("user_id", lastItemID, nil, nil)
//		}
//		items, err := h.db.GetRecordsWithScanFilter(ctx, User{}, filter)
//		if err != nil {
//			// you should handle the error, as for what is written to show the usage
//			ch <- User{}
//			return
//		}
//
//		for _, item := range items {
//			user, ok := item.(User)
//			if !ok {
//				// you should handle the error, as for what is written to show the usage
//				ch <- User{}
//				return
//			}
//			// everything is fine
//			ch <- user
//		}
//	}
//	func (h dbHandler) getAllByScan(ctx context.Context, pageSize int, lastItemID string, ch chan<- User) {
//		// please note that this is a query which is efficient in terms of cost and time
//		// you can use filter operation on indexed fields to reduce the data size
//		// the results will be paginated. please refer to aws limits for scan queries
//		// use it with caution
//		defer close(ch)
//
//		filter := NewExpressionWrapper(h.config.TableInfo.TableName).
//			WithKeyCondition("first_name", "saddam", EQUAL).
//			AndCondition("last_name", "abu", EQUAL).
//			WithLimit(int64(pageSize))
//		if lastItemID != "" {
//			filter.WithLastEvaluatedKey("user_id", lastItemID, nil, nil)
//		}
//		items, err := h.db.GetRecordsWithQueryFilter(ctx, User{}, filter)
//		if err != nil {
//			// you should handle the error, as for what is written to show the usage
//			ch <- User{}
//			return
//		}
//
//		for _, item := range items {
//			user, ok := item.(User)
//			if !ok {
//				// you should handle the error, as for what is written to show the usage
//				ch <- User{}
//				return
//			}
//			// everything is fine
//			ch <- user
//		}
//	}
//
// main entry point initialize dynamo config along with the repository
//	func main() {
//		//some db configuration is needed to map the partition keys and sorting key to the correspondent table and indices
//		sKey = "email_address"
//		config := DBConfig{
//			TableInfo: DBTableInfo{
//				TableName: "user",
//				DBPSKeyNames: DBPSKeyNames{
//					PartitionKey: "user_id",
//					SortKey:      &sKey,
//				},
//			},
//			Indexes: map[DynamoTableOrIndexName]DBPSKeyNames{
//				"user_by_email_index": {
//					PartitionKey: "email_address",
//					SortKey:      nil,
//				},
//			},
//		}
//		// initialize DynamoDB connection handler
//		db, err := NewDynamoDB(config)
//		if err != nil {
//			log.Fatal("unable to load db configuration")
//		}
//		repo := newUserRepo(db, config)
//
//		// then you could use the repo functions
//		// please refer to query and command tests to get deeper understanding of the DynamoDB abstraction
//		user, _ := repo.getUserByID(context.TODO(), "123")
//		fmt.Println(user.FirstName)
//	}
package dynamodb
