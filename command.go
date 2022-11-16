package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
)

func (h handlerImp) AddRecord(ctx context.Context, in BaseModel, createSortKey bool) (DBPSKeyValues, error) {
	item, keys, err := h.createPutItem(in, true, createSortKey)
	if err != nil {
		return nil, err
	}
	tabInfo := h.config.TableInfo
	// create the put request
	input := dynamodb.PutItemInput{
		Item:                item,
		TableName:           aws.String(tabInfo.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%v)", tabInfo.PartitionKey)),
	}
	// triggering the put operation
	_, err = h.PutItemWithContext(ctx, &input)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (h handlerImp) UpdateRecordByID(ctx context.Context, in BaseModel, dbKeys DBPSKeyValues) error {
	tabInfo := h.config.TableInfo

	if tabInfo.SortKey != nil && dbKeys.GetSortKey() == nil {
		return errors.New("missing required sorting key")
	}
	// marshaling the input
	item, err := in.Marshal()
	if err != nil {
		return err
	}

	partitionKey := string(dbKeys.GetPartitionKey())

	// defining the partition and sort keys
	item[string(tabInfo.PartitionKey)] = &dynamodb.AttributeValue{
		S: aws.String(partitionKey),
	}
	if tabInfo.SortKey != nil {
		sortKey := string(*dbKeys.GetSortKey())
		item[string(*tabInfo.SortKey)] = &dynamodb.AttributeValue{
			S: aws.String(sortKey),
		}
	}
	// create the put request
	input := dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tabInfo.TableName),
	}
	// triggering the put operation
	_, err = h.PutItemWithContext(ctx, &input)
	return err
}

// Update a dynamo item attributes
//  - to update the entire item the [data] map need to be populated with all item fields.
//  - to update some fields only the fields to be updated need to be provided.
func (h handlerImp) Update(ctx context.Context, partKey string, sortKey *string, data map[FieldName]interface{}) error {
	tabInfo := h.config.TableInfo

	if tabInfo.SortKey != nil && sortKey == nil {
		return errors.New("missing required sorting key")
	}

	builder := NewExpressionWrapper(tabInfo.TableName)
	builder.WithPartitionKey(string(tabInfo.PartitionKey), partKey)

	if sortKey != nil {
		builder.WithSortingKey(string(*tabInfo.SortKey), *sortKey)
	}

	for k, v := range data {
		builder.WithUpdateField(string(k), v)
	}

	updateRequest, err := builder.BuildUpdateInput()
	if err != nil {
		return err
	}

	_, err = h.UpdateItemWithContext(ctx, updateRequest)
	return err
}

// DeleteRecordByID deletes a record from dynamo db for the defined dbKeys if the provided filter is matched
func (h handlerImp) DeleteRecordByID(ctx context.Context, dbKeys DBPSKeyValues, filters *AwsExpressionWrapper) error {
	tabInfo := h.config.TableInfo
	// check for required attributes
	if len(dbKeys.GetPartitionKey()) < 1 {
		return errors.New("missing required partition key")
	}
	if tabInfo.SortKey != nil && dbKeys.GetSortKey() == nil {
		return errors.New("missing required sort key")
	}

	if filters == nil {
		filters = NewExpressionWrapper(tabInfo.TableName)
	}

	filters.WithPartitionKey(string(tabInfo.PartitionKey), string(dbKeys.GetPartitionKey()))
	if tabInfo.SortKey != nil {
		filters.WithSortingKey(string(*tabInfo.SortKey), string(*dbKeys.GetSortKey()))
	}

	req, err := filters.BuildDeleteInput()
	if err != nil {
		return err
	}
	_, err = h.DeleteItemWithContext(ctx, req)
	return err
}

func (h handlerImp) BulkAddRecords(ctx context.Context, baseModel BaseModel, createSortKey bool, records ...BaseModel) ([]BaseModel, error) {
	return h.batchWrite(ctx, baseModel, records, true, createSortKey)
}

// BulkUpdateRecords updates multiple DynamoDB records
func (h handlerImp) BulkUpdateRecords(ctx context.Context, baseModel BaseModel, records ...BaseModel) ([]BaseModel, error) {
	return h.batchWrite(ctx, baseModel, records, false, false)
}

// BulkDeleteRecords delete a bulk of dynamo records
func (h handlerImp) BulkDeleteRecords(ctx context.Context, dbKeys ...DBPSKeyValues) ([]DBPSKeyValues, error) {
	tabInfo := h.config.TableInfo
	tableKeys := tabInfo.DBPSKeyNames

	items := make([]*dynamodb.WriteRequest, 0, len(dbKeys))

	for _, key := range dbKeys {
		expr := NewExpressionWrapper(h.config.TableInfo.TableName).
			WithPartitionKey(string(tableKeys.PartitionKey), string(key.GetPartitionKey()))

		if tableKeys.SortKey != nil && key.GetSortKey() == nil {
			return dbKeys, errors.New("missing required sort key")
		}
		if tableKeys.SortKey != nil && key.GetSortKey() != nil {
			expr.WithSortingKey(string(*tableKeys.SortKey), string(*key.GetSortKey()))
		}

		attribute, err := expr.CreateQueryKeys()
		if err != nil {
			return dbKeys, err
		}

		item := &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: attribute,
			},
		}
		items = append(items, item)
	}
	requests := map[string][]*dynamodb.WriteRequest{
		tabInfo.TableName: items,
	}
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: requests,
	}
	out, err := h.BatchWriteItemWithContext(ctx, input)
	if err != nil {
		return dbKeys, err
	}

	unprocessedItems := make([]DBPSKeyValues, 0, len(dbKeys))
	for _, item := range out.UnprocessedItems[tabInfo.TableName] {
		dbKey := dbPSKeyValues{}
		var partKey string
		var sortKey string

		_ = dynamodbattribute.Unmarshal(item.DeleteRequest.Key[string(tabInfo.PartitionKey)], &partKey)
		_ = dynamodbattribute.Unmarshal(item.DeleteRequest.Key[string(*tabInfo.SortKey)], &sortKey)

		dbKey.partitionKey = DBKeyValue(partKey)
		if sortKey != "" {
			s := DBKeyValue(sortKey)
			dbKey.sortKey = &s
		}

		unprocessedItems = append(unprocessedItems, dbKey)
	}
	return unprocessedItems, nil
}

func (h handlerImp) batchWrite(ctx context.Context, baseModel BaseModel, records []BaseModel, createPartKey, createSortKey bool) ([]BaseModel, error) {
	max := int(math.Min(25, float64(len(records))))
	requests := make([]*dynamodb.WriteRequest, 0, max)

	for _, rec := range records[:max] {
		item, _, err := h.createPutItem(rec, createPartKey, createSortKey)
		if err != nil {
			return records, err
		}
		req := dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: item},
		}
		requests = append(requests, &req)
	}

	bInput := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			h.config.TableInfo.TableName: requests,
		},
	}

	unprocessedItems := records[max:]
	res, err := h.BatchWriteItemWithContext(ctx, &bInput)
	if err != nil {
		return records, err
	}
	for _, item := range res.UnprocessedItems[h.config.TableInfo.TableName] {
		dynamoItem := item.PutRequest.Item
		rec, err := baseModel.Unmarshal(dynamoItem)
		if err != nil {
			return records, err
		}
		unprocessedItems = append(unprocessedItems, rec)
	}

	return unprocessedItems, nil
}

func (h handlerImp) createPutItem(in BaseModel, createPartKey bool, createSortKey bool) (DBMap, DBPSKeyValues, error) {
	// marshaling the input
	item, err := in.Marshal()
	if err != nil {
		return nil, nil, err
	}

	tabInfo := h.config.TableInfo
	partitionKey := in.GetPartSortKey(nil).GetPartitionKey()

	sortKey := in.GetPartSortKey(nil).GetSortKey()

	if partitionKey == "" && !createPartKey {
		return nil, nil, errors.New("missing required partition key")
	}

	if partitionKey == "" {
		partitionKey = DBKeyValue(uuid.New().String())
	}

	// defining the partition and sort keys
	item[string(tabInfo.PartitionKey)] = &dynamodb.AttributeValue{
		S: aws.String(string(partitionKey)),
	}
	if tabInfo.SortKey != nil && sortKey == nil && !createSortKey {
		return nil, nil, errors.New("missing required sorting key")
	}

	if tabInfo.SortKey != nil && sortKey == nil {
		key := DBKeyValue(uuid.New().String())
		sortKey = &key
	}
	if tabInfo.SortKey != nil && sortKey != nil {
		item[string(*tabInfo.SortKey)] = &dynamodb.AttributeValue{
			S: aws.String(string(*sortKey)),
		}
	}
	keys := dbPSKeyValues{
		partitionKey: partitionKey,
		sortKey:      sortKey,
	}
	return item, keys, nil
}
