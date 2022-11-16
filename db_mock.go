package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// MockedGetItem ..
type MockedGetItem struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.GetItemOutput
	Err  error
}

// GetItemWithContext mocks GetItemWithContext
func (m MockedGetItem) GetItemWithContext(_ aws.Context, _ *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return &m.Resp, nil
}

// MockQuery ..
type MockQuery struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.QueryOutput
	Err  error
}

// QueryWithContext mocks QueryWithContext
func (m MockQuery) QueryWithContext(aws.Context, *dynamodb.QueryInput, ...request.Option) (*dynamodb.QueryOutput, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return &m.Resp, nil
}

// MockedBatchGet ..
type MockedBatchGet struct {
	dynamodbiface.DynamoDBAPI
	TableName       string
	IgnoreTableName string
	Resp            dynamodb.BatchGetItemOutput
	Err             error
}

// BatchGetItemWithContext mocks dynamo's BatchGetItemWithContext
func (bg MockedBatchGet) BatchGetItemWithContext(_ aws.Context, in *dynamodb.BatchGetItemInput, _ ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	if in.RequestItems[bg.TableName] != nil && len(in.RequestItems[bg.TableName].Keys) == 0 {
		return &dynamodb.BatchGetItemOutput{}, nil
	}

	if _, ok := in.RequestItems[bg.IgnoreTableName]; ok {
		bg.Resp.UnprocessedKeys = map[string]*dynamodb.KeysAndAttributes{}
	}
	return &bg.Resp, bg.Err
}

// MockedPutItem  ..
type MockedPutItem struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.PutItemOutput
	Err  error
}

// PutItemWithContext mocks dynamo's PutItemWithContext
func (mw MockedPutItem) PutItemWithContext(aws.Context, *dynamodb.PutItemInput, ...request.Option) (*dynamodb.PutItemOutput, error) {
	if mw.Err != nil {
		return nil, mw.Err
	}
	return &mw.Resp, nil
}

// MockedUpdateItem ..
type MockedUpdateItem struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.UpdateItemOutput
	Err  error
}

// UpdateItemWithContext mocks dynamo's UpdateItemWithContext
func (m MockedUpdateItem) UpdateItemWithContext(aws.Context, *dynamodb.UpdateItemInput, ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return &m.Resp, m.Err
}

// MockDeleteItem ...
type MockDeleteItem struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.DeleteItemOutput
	Err  error
}

// DeleteItemWithContext mocks dynamodb's DeleteItemWithContext
func (d MockDeleteItem) DeleteItemWithContext(aws.Context, *dynamodb.DeleteItemInput, ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	if d.Err != nil {
		return nil, d.Err
	}
	return &d.Resp, nil
}

// MockedBatchWrite ..
type MockedBatchWrite struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.BatchWriteItemOutput
	Err  error
}

// BatchWriteItemWithContext mocks dynamo's BatchWriteItemWithContext
func (bw MockedBatchWrite) BatchWriteItemWithContext(aws.Context, *dynamodb.BatchWriteItemInput, ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	if bw.Err != nil {
		return nil, bw.Err
	}
	return &bw.Resp, nil
}

// MockScan ...
type MockScan struct {
	dynamodbiface.DynamoDBAPI
	Resp dynamodb.ScanOutput
	Err  error
}

// ScanWithContext mocks dynamodb's ScanWithContext
func (m MockScan) ScanWithContext(aws.Context, *dynamodb.ScanInput, ...request.Option) (*dynamodb.ScanOutput, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return &m.Resp, nil
}
