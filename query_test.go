package dynamodb

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/assert"
)

// BaseModel implements the BaseModel
type TestBaseModel struct {
	Name               string
	Age                int
	SKey               string
	withMarshallingErr bool
}

// GetModelType returns the model type
func (mdl TestBaseModel) GetModelType() DBModelName {
	return "TestBaseModel"
}

// Marshal Serialize translates the model to dynamodb object
func (mdl TestBaseModel) Marshal() (DBMap, error) {
	if mdl.withMarshallingErr {
		return nil, errors.New("marshalling error")
	}

	return dynamodbattribute.MarshalMap(mdl)
}

// Unmarshal deserialize translates the dynamodb object to golang object
func (mdl TestBaseModel) Unmarshal(data DBMap) (BaseModel, error) {
	if mdl.withMarshallingErr {
		return nil, errors.New("unmarshalling error")
	}
	err := dynamodbattribute.UnmarshalMap(data, &mdl)

	if mdl == (TestBaseModel{}) {
		return nil, errors.New("error marshaling")
	}
	return mdl, err
}

// GetPartSortKey return partition key and sorting key if available
func (mdl TestBaseModel) GetPartSortKey(_ *DynamoTableOrIndexName) DBPSKeyValues {
	partKey := DBKeyValue(mdl.Name)
	var sortKey *DBKeyValue
	if mdl.SKey != "" {
		key := DBKeyValue(mdl.SKey)
		sortKey = &key
	}
	return dbPSKeyValues{
		partitionKey: partKey,
		sortKey:      sortKey,
	}
}

func TestNewDbPSKeyValues(t *testing.T) {
	pKey := DBKeyValue("partitionKey")
	sortKey := DBKeyValue("sortKey")
	keys := NewDbPSKeyValues(pKey, &sortKey)
	assert.Equal(t, pKey, keys.GetPartitionKey())
	assert.NotNil(t, keys.GetSortKey())
	assert.Equal(t, sortKey, *keys.GetSortKey())
}

func TestHandler_GetByID(t *testing.T) {
	expectedName := "golang"
	expectedAge := 12
	validGetResp := dynamodb.GetItemOutput{
		Item: DBMap{
			"name": &dynamodb.AttributeValue{
				S: aws.String(expectedName),
			},
			"Age": &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(expectedAge)),
			},
		},
	}
	validPKey := DBKeyValue("pKey")
	validSKey := DBKeyValue("sKey")

	cases := []struct {
		Name     string
		Req      dbPSKeyValues
		Index    DynamoTableOrIndexName
		Resp     dynamodb.GetItemOutput
		DbErr    error
		hasError bool
	}{
		{
			Name: "successfully",
			Req: dbPSKeyValues{
				partitionKey: validPKey,
				sortKey:      &validSKey,
			},
			Resp: validGetResp,
		},
		{
			Name: "successfully",
			Req: dbPSKeyValues{
				partitionKey: validPKey,
			},
			Index: "index",
			Resp:  validGetResp,
		},
		{
			Name:     "with empty request",
			Req:      dbPSKeyValues{},
			Resp:     validGetResp,
			hasError: true,
		},
		{
			Name: "nil response",
			Req: dbPSKeyValues{
				partitionKey: validPKey,
			},
			Resp: dynamodb.GetItemOutput{
				Item: nil,
			},
		},
		{
			Name: "empty response",
			Req: dbPSKeyValues{
				partitionKey: validPKey,
			},
			Resp: dynamodb.GetItemOutput{},
		},
		{
			Name: "with db error",
			Req: dbPSKeyValues{
				partitionKey: validPKey,
			},
			DbErr:    errors.New("db error"),
			hasError: true,
		},
	}
	t.Run("successfully with partition key only", func(t *testing.T) {
		config := DBConfig{
			TableInfo: DBTableInfo{
				TableName: "table",
				DBPSKeyNames: DBPSKeyNames{
					PartitionKey: DBKeyName("pkey"),
				},
			},
		}

		repo := handlerImp{
			config: config,
			DynamoDBAPI: MockedGetItem{
				Resp: validGetResp,
			},
		}
		mdl := TestBaseModel{}

		ctx := context.Background()
		index := DynamoTableOrIndexName("index")
		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")
		res, err := repo.GetByID(ctx, mdl, index, dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		})

		assert.Nil(t, err)
		assert.NotEmpty(t, res)
		assert.Equal(t, expectedName, (res).(TestBaseModel).Name)
		assert.Equal(t, expectedAge, (res).(TestBaseModel).Age)
	})

	for i, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			repo := handlerImp{
				config: cfg,
				DynamoDBAPI: MockedGetItem{
					Resp: tc.Resp,
					Err:  tc.DbErr,
				},
			}

			mdl := TestBaseModel{}

			ctx := context.Background()
			res, err := repo.GetByID(ctx, mdl, tc.Index, tc.Req)
			assert.True(t, (tc.hasError && err != nil) || (!tc.hasError && err == nil))

			if i == 0 && res == nil {
				t.Error("expected a response got nil")
			}

			if i == 0 && res != nil {
				assert.NotEmpty(t, res)
				assert.Equal(t, expectedName, (res).(TestBaseModel).Name)
				assert.Equal(t, expectedAge, (res).(TestBaseModel).Age)
			}
		})
	}
}

func TestHandler_GetRecordsWithScanFilter(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Logf("recovered from panic %v", r)
		}
	}()

	createValidResp := func(name string, age int) []map[string]*dynamodb.AttributeValue {
		return []map[string]*dynamodb.AttributeValue{
			{
				"name": &dynamodb.AttributeValue{
					S: aws.String(name),
				},
				"Age": &dynamodb.AttributeValue{
					N: aws.String(strconv.Itoa(age)),
				},
			},
		}
	}

	t.Run("successfully", func(t *testing.T) {
		mdl := TestBaseModel{}
		expectedName := "golang"
		expectedAge := 12

		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockScan{
				Resp: dynamodb.ScanOutput{
					Items: createValidResp(expectedName, expectedAge),
					LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
						"name": {
							S: &expectedName,
						},
					},
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithScanFilter(ctx, &mdl, req)
		assert.NotNil(t, dbAttrVal)
		assert.NoError(t, err)
		assert.NotEmpty(t, res)
		assert.Equal(t, expectedName, res[0].(TestBaseModel).Name)
		assert.Equal(t, expectedAge, res[0].(TestBaseModel).Age)
	})

	t.Run("with empty response", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockScan{
				Resp: dynamodb.ScanOutput{
					Items:            []map[string]*dynamodb.AttributeValue{},
					LastEvaluatedKey: nil,
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithScanFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("with an error", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockScan{
				Err: errors.New("custom error"),
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithScanFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("with unmarshalling error", func(t *testing.T) {
		mdl := TestBaseModel{withMarshallingErr: true}

		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockScan{
				Resp: dynamodb.ScanOutput{
					Items: createValidResp("name", 5),
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithScanFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Empty(t, res)
	})

	t.Run("with an error while creating the request", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockScan{
				Resp: dynamodb.ScanOutput{
					Items: createValidResp("name", 5),
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper("")

		res, dbAttrVal, err := repo.GetRecordsWithScanFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

}

func TestHandler_GetRecordsWithQueryFilter(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Logf("recovered from panic %v", r)
		}
	}()

	createValidResp := func(name string, age int) []map[string]*dynamodb.AttributeValue {
		return []map[string]*dynamodb.AttributeValue{
			{
				"name": &dynamodb.AttributeValue{
					S: aws.String(name),
				},
				"Age": &dynamodb.AttributeValue{
					N: aws.String(strconv.Itoa(age)),
				},
			},
		}
	}

	t.Run("successfully", func(t *testing.T) {
		mdl := TestBaseModel{}
		expectedName := "golang"
		expectedAge := 12

		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockQuery{
				Resp: dynamodb.QueryOutput{
					Items: createValidResp(expectedName, expectedAge),
					LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
						"name": {
							S: &expectedName,
						},
					},
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithQueryFilter(ctx, &mdl, req)
		assert.NotNil(t, dbAttrVal)
		assert.NoError(t, err)
		assert.NotEmpty(t, res)
		assert.Equal(t, expectedName, res[0].(TestBaseModel).Name)
		assert.Equal(t, expectedAge, res[0].(TestBaseModel).Age)
	})

	t.Run("with empty response", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockQuery{
				Resp: dynamodb.QueryOutput{
					Items:            []map[string]*dynamodb.AttributeValue{},
					LastEvaluatedKey: nil,
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithQueryFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("with an error", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockQuery{
				Err: errors.New("custom error"),
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithQueryFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("with unmarshalling error", func(t *testing.T) {
		mdl := TestBaseModel{withMarshallingErr: true}

		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockQuery{
				Resp: dynamodb.QueryOutput{
					Items: createValidResp("name", 5),
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper(cfg.TableInfo.TableName).
			WithKeyCondition("pName", "pValue", EQUAL)

		res, dbAttrVal, err := repo.GetRecordsWithQueryFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Empty(t, res)
	})

	t.Run("with an error while creating the request", func(t *testing.T) {
		mdl := TestBaseModel{}
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockQuery{
				Resp: dynamodb.QueryOutput{
					Items: createValidResp("name", 5),
				},
				Err: nil,
			},
		}

		ctx := context.Background()
		req := NewExpressionWrapper("")

		res, dbAttrVal, err := repo.GetRecordsWithQueryFilter(ctx, &mdl, req)
		assert.Nil(t, dbAttrVal)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

}

func TestHandler_GetByIDs(t *testing.T) {
	expectedName := "golang"
	expectedAge := 12
	validItem := DBMap{
		"name": &dynamodb.AttributeValue{
			S: aws.String(expectedName),
		},
		"Age": &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(expectedAge)),
		},
	}

	t.Run("successfully", func(t *testing.T) {
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockedBatchGet{
				TableName: cfg.TableInfo.TableName,
				Resp: dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]*dynamodb.AttributeValue{
						cfg.TableInfo.TableName: {
							validItem,
						},
					},
				},
			},
		}
		mdl := TestBaseModel{}

		ctx := context.Background()
		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")

		res, err := repo.GetByIDs(ctx, mdl, []DBPSKeyValues{dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		}})

		assert.Nil(t, err)
		assert.NotEmpty(t, res)
	})

	t.Run("with error", func(t *testing.T) {
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockedBatchGet{
				Err: errors.New("fake error"),
			},
		}
		mdl := TestBaseModel{}

		ctx := context.Background()
		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")

		res, err := repo.GetByIDs(ctx, mdl, []DBPSKeyValues{dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		}})

		assert.Error(t, err)
		assert.Empty(t, res)
	})

	t.Run("with marshaling error", func(t *testing.T) {
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockedBatchGet{
				Resp: dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]*dynamodb.AttributeValue{
						cfg.TableInfo.TableName: {
							validItem,
						},
					},
				},
			},
		}
		mdl := TestBaseModel{withMarshallingErr: true}

		ctx := context.Background()
		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")

		res, err := repo.GetByIDs(ctx, mdl, []DBPSKeyValues{dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		}})

		assert.Error(t, err)
		assert.Empty(t, res)
	})

	t.Run("with wrong input (missing partition key)", func(t *testing.T) {
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockedBatchGet{
				TableName: cfg.TableInfo.TableName,
				Resp: dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]*dynamodb.AttributeValue{
						cfg.TableInfo.TableName: {
							validItem,
						},
					},
				},
			},
		}
		mdl := TestBaseModel{}

		ctx := context.Background()
		sKey := DBKeyValue("sKey")

		res, err := repo.GetByIDs(ctx, mdl, []DBPSKeyValues{dbPSKeyValues{
			sortKey: &sKey,
		}})

		assert.NoError(t, err)
		assert.Empty(t, res)
	})

	t.Run("successfully with unprocessed keys", func(t *testing.T) {
		repo := handlerImp{
			config: cfg,
			DynamoDBAPI: MockedBatchGet{
				TableName:       cfg.TableInfo.TableName,
				IgnoreTableName: "ignore",
				Resp: dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]*dynamodb.AttributeValue{
						cfg.TableInfo.TableName: {
							validItem,
						},
					},
					UnprocessedKeys: map[string]*dynamodb.KeysAndAttributes{
						"ignore": {
							Keys: []map[string]*dynamodb.AttributeValue{
								{
									"pkey": &dynamodb.AttributeValue{
										S: aws.String("pkey"),
									},
								},
							},
						},
					},
				},
			},
		}
		mdl := TestBaseModel{}

		ctx := context.Background()
		pKey := DBKeyValue("pKey")
		sKey := DBKeyValue("sKey")

		res, err := repo.GetByIDs(ctx, mdl, []DBPSKeyValues{dbPSKeyValues{
			partitionKey: pKey,
			sortKey:      &sKey,
		}})

		assert.Nil(t, err)
		assert.NotEmpty(t, res)
	})
}
