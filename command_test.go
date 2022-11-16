package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/bxcodec/faker/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type bulkWriteTestData struct {
	name                   string
	in                     []BaseModel
	dbResp                 dynamodb.BatchWriteItemOutput
	dbError                error
	unprocessedItemCount   int
	shouldNotCreateSortKey bool
	hasError               bool
}

type method int

const bulkAdd = method(1)
const bulkUpdate = method(2)

func TestHandlerImp_AddRecord(t *testing.T) {
	cases := []struct {
		name          string
		input         TestBaseModel
		createSortKey bool
		dbError       error
		hasError      bool
	}{
		{
			name: "successfully",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			createSortKey: true,
		},
		{
			name: "with db error",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			createSortKey: true,
			dbError:       errors.New("fake error"),
			hasError:      true,
		},
		{
			name:          "with unmarshalling error",
			input:         TestBaseModel{withMarshallingErr: true},
			createSortKey: true,
			hasError:      true,
		},
		{
			name: "missing required sort key",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			hasError: true,
		},
		{
			name: "with required sort key",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
				SKey: "key",
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			//t.Parallel()
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedPutItem{
					Resp: dynamodb.PutItemOutput{},
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()
			res, err := repo.AddRecord(ctx, &tc.input, tc.createSortKey)
			assert.True(t, tc.hasError == (err != nil), fmt.Sprintf("%v", err))
			if res != nil {
				assert.NotEmpty(t, res.GetPartitionKey())
				assert.NotEmpty(t, res.GetSortKey())
			}
		})
	}
}

func TestHandlerImp_UpdateRecordByID(t *testing.T) {
	validDBKeys := dbPSKeyValues{
		partitionKey: "part",
		sortKey: func() *DBKeyValue {
			sKey := DBKeyValue("sKey")
			return &sKey
		}(),
	}
	cases := []struct {
		name     string
		input    TestBaseModel
		dbKeys   dbPSKeyValues
		dbError  error
		hasError bool
	}{
		{
			name: "successfully",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			dbKeys: validDBKeys,
		},
		{
			name: "missing required sort key",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			dbKeys: dbPSKeyValues{
				partitionKey: "part",
			},
			hasError: true,
		},
		{
			name: "with db error",
			input: TestBaseModel{
				Name: "golang",
				Age:  12,
			},
			dbKeys:   validDBKeys,
			dbError:  errors.New("fake error"),
			hasError: true,
		},
		{
			name:     "with unmarshalling error",
			input:    TestBaseModel{withMarshallingErr: true},
			dbKeys:   validDBKeys,
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedPutItem{
					Resp: dynamodb.PutItemOutput{},
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()
			err := repo.UpdateRecordByID(ctx, &tc.input, tc.dbKeys)
			assert.True(t, tc.hasError == (err != nil), fmt.Sprintf("%v", err))
		})
	}
}

func TestHandlerImp_Update(t *testing.T) {
	cases := []struct {
		name     string
		input    map[FieldName]interface{}
		partKey  string
		sortKey  *string
		dbError  error
		hasError bool
	}{
		{
			name: "successfully",
			input: map[FieldName]interface{}{
				"name": "golang",
			},
			partKey: uuid.NewString(),
			sortKey: func() *string {
				id := uuid.NewString()
				return &id
			}(),
		},
		{
			name: "with empty partition key",
			input: map[FieldName]interface{}{
				"name": "golang",
			},
			partKey: "",
			sortKey: func() *string {
				id := uuid.NewString()
				return &id
			}(),
			hasError: true,
		},
		{
			name: "missing required sort key",
			input: map[FieldName]interface{}{
				"name": "golang",
			},
			partKey:  uuid.NewString(),
			hasError: true,
		},
		{
			name: "with db error",
			input: map[FieldName]interface{}{
				"name": "golang",
			},
			partKey: uuid.NewString(),
			sortKey: func() *string {
				id := uuid.NewString()
				return &id
			}(),
			dbError:  errors.New("fake error"),
			hasError: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedUpdateItem{
					Err: tc.dbError,
				},
			}
			ctx := context.Background()
			err := repo.Update(ctx, tc.partKey, tc.sortKey, tc.input)
			assert.True(t, tc.hasError == (err != nil), fmt.Sprintf("%v", err))
		})
	}
}

func TestHandlerImp_DeleteRecordByID(t *testing.T) {
	cases := []struct {
		name     string
		dbKeys   dbPSKeyValues
		filters  *AwsExpressionWrapper
		dbError  error
		hasError bool
	}{
		{
			name: "successfully",
			dbKeys: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
		},
		{
			name: "successfully with filters",
			dbKeys: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			filters: NewExpressionWrapper("tableName").
				AndCondition("param", "132", LT),
		},
		{
			name: "without partition key",
			dbKeys: dbPSKeyValues{
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			hasError: true,
		},
		{
			name: "without sorting key",
			dbKeys: dbPSKeyValues{
				partitionKey: "partKey",
			},
			hasError: true,
		},
		{
			name: "with db error",
			dbKeys: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			dbError:  errors.New("fake error"),
			hasError: true,
		},
	}

	t.Run("with wrong config", func(t *testing.T) {
		config := DBConfig{}
		repo := handlerImp{
			config: config,
			DynamoDBAPI: MockDeleteItem{
				Resp: dynamodb.DeleteItemOutput{},
			},
		}
		ctx := context.Background()

		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")

		dbKeys := dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		}

		err := repo.DeleteRecordByID(ctx, dbKeys, nil)
		assert.Error(t, err)
	})

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockDeleteItem{
					Resp: dynamodb.DeleteItemOutput{},
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()
			err := repo.DeleteRecordByID(ctx, tc.dbKeys, tc.filters)
			assert.True(t, tc.hasError == (err != nil), fmt.Sprintf("%v", err))
		})
	}
}

func TestHandlerImp_BulkAddRecords(t *testing.T) {
	cases := getBulkWriteTestData(bulkAdd)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedBatchWrite{
					Resp: tc.dbResp,
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()

			unpressedItems, err := repo.BulkAddRecords(ctx, TestBaseModel{}, !tc.shouldNotCreateSortKey, tc.in...)
			assert.Equal(t, tc.hasError, err != nil)
			assert.Equal(t, tc.unprocessedItemCount, len(unpressedItems))
		})
	}
}

func TestHandlerImp_BulkUpdateRecords(t *testing.T) {
	cases := getBulkWriteTestData(bulkUpdate)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedBatchWrite{
					Resp: tc.dbResp,
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()

			unpressedItems, err := repo.BulkUpdateRecords(ctx, TestBaseModel{}, tc.in...)
			assert.Equal(t, tc.hasError, err != nil)
			assert.Equal(t, tc.unprocessedItemCount, len(unpressedItems))
		})
	}
}

func TestHandlerImp_BulkDeleteRecords(t *testing.T) {
	validItem := DBMap{
		string(cfg.TableInfo.PartitionKey): &dynamodb.AttributeValue{
			S: aws.String("test"),
		},
		string(*cfg.TableInfo.SortKey): &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(12)),
		},
	}
	cases := []struct {
		name                 string
		cfg                  DBConfig
		in                   dbPSKeyValues
		dbResp               dynamodb.BatchWriteItemOutput
		dbError              error
		unprocessedItemCount int
		hasErr               bool
	}{
		{
			name: "successfully",
			cfg:  cfg,
			in: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			dbResp: dynamodb.BatchWriteItemOutput{},
		},
		{
			name: "with unprocessed item",
			cfg:  cfg,
			in: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			dbResp: dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					cfg.TableInfo.TableName: {
						&dynamodb.WriteRequest{
							DeleteRequest: &dynamodb.DeleteRequest{
								Key: validItem,
							},
						},
					},
				},
			},
			unprocessedItemCount: 1,
		},
		{
			name: "with db error",
			cfg:  cfg,
			in: dbPSKeyValues{
				partitionKey: "partKey",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			dbResp:               dynamodb.BatchWriteItemOutput{},
			dbError:              errors.New("db error"),
			hasErr:               true,
			unprocessedItemCount: 1,
		},
		{
			name: "with empty partition-key value",
			cfg:  cfg,
			in: dbPSKeyValues{
				partitionKey: "",
				sortKey: func() *DBKeyValue {
					sKey := DBKeyValue("sKey")
					return &sKey
				}(),
			},
			dbResp:               dynamodb.BatchWriteItemOutput{},
			hasErr:               true,
			unprocessedItemCount: 1,
		},
		{
			name: "with missing sort key",
			cfg:  cfg,
			in: dbPSKeyValues{
				partitionKey: "part-key",
			},
			dbResp:               dynamodb.BatchWriteItemOutput{},
			hasErr:               true,
			unprocessedItemCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedBatchWrite{
					Resp: tc.dbResp,
					Err:  tc.dbError,
				},
			}
			ctx := context.Background()
			out, err := repo.BulkDeleteRecords(ctx, tc.in)
			assert.Equal(t, tc.hasErr, err != nil)
			assert.Equal(t, tc.unprocessedItemCount, len(out))
		})
	}
}

func getBulkWriteTestData(met method) []bulkWriteTestData {
	cases := []bulkWriteTestData{
		{
			name:                 "successfully",
			in:                   generateTestData(29),
			dbResp:               dynamodb.BatchWriteItemOutput{},
			unprocessedItemCount: 4,
		},
		{
			name: "with unprocessed items",
			in:   generateTestData(29),
			dbResp: dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					cfg.TableInfo.TableName: {
						&dynamodb.WriteRequest{
							PutRequest: &dynamodb.PutRequest{
								Item: DBMap{
									"name": &dynamodb.AttributeValue{
										S: aws.String("test"),
									},
									"Age": &dynamodb.AttributeValue{
										N: aws.String(strconv.Itoa(12)),
									},
									"sKey": &dynamodb.AttributeValue{
										S: aws.String("test"),
									},
								},
							},
						},
					},
				},
			},
			unprocessedItemCount: 5,
		},
		{
			name:                 "with db error",
			in:                   generateTestData(1),
			dbResp:               dynamodb.BatchWriteItemOutput{},
			dbError:              errors.New("db error"),
			unprocessedItemCount: 1,
			hasError:             true,
		},
		{
			name: "with marshaling error",
			in: []BaseModel{
				TestBaseModel{
					Name:               "golang",
					Age:                12,
					withMarshallingErr: true,
				},
			},
			dbResp:               dynamodb.BatchWriteItemOutput{},
			unprocessedItemCount: 1,
			hasError:             true,
		},
		{
			name: "with missing mandatory sort key error",
			in: []BaseModel{
				TestBaseModel{
					Name: "golang",
					Age:  12,
				},
			},
			shouldNotCreateSortKey: true,
			dbResp:                 dynamodb.BatchWriteItemOutput{},
			unprocessedItemCount:   1,
			hasError:               true,
		},
		{
			name: "with un-marshalable unprocessed items",
			in:   generateTestData(1),
			dbResp: dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					cfg.TableInfo.TableName: {
						&dynamodb.WriteRequest{
							PutRequest: &dynamodb.PutRequest{
								Item: DBMap{
									"Test": &dynamodb.AttributeValue{
										S: aws.String("test"),
									},
									"Data": &dynamodb.AttributeValue{
										N: aws.String(strconv.Itoa(12)),
									},
								},
							},
						},
					},
				},
			},
			unprocessedItemCount: 1,
			hasError:             true,
		},
	}
	if met == bulkAdd {
		cases = append(cases,
			bulkWriteTestData{
				name: "without partition key",
				in: []BaseModel{
					TestBaseModel{},
				},
				dbResp:   dynamodb.BatchWriteItemOutput{},
				hasError: false,
			})
	}

	if met == bulkUpdate {
		cases = append(cases,
			bulkWriteTestData{
				name: "without partition key",
				in: []BaseModel{
					TestBaseModel{},
				},
				dbResp:               dynamodb.BatchWriteItemOutput{},
				hasError:             true,
				unprocessedItemCount: 1,
			})
	}

	return cases
}
func generateTestData(size int) []BaseModel {
	models := make([]BaseModel, 0, size)
	for i := 0; i < size; i++ {
		mdl := TestBaseModel{}
		_ = faker.FakeData(&mdl)
		mdl.withMarshallingErr = false
		models = append(models, mdl)
	}
	return models
}
