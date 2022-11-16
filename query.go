package dynamodb

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (h handlerImp) GetByID(ctx context.Context, input BaseModel, index DynamoTableOrIndexName, dbKeys DBPSKeyValues) (BaseModel, error) {
	req, err := h.prepareGetReq(index, dbKeys)
	if err != nil {
		return nil, err
	}

	res, getErr := h.GetItemWithContext(ctx, req)
	if getErr != nil {
		return nil, getErr
	}

	if len(res.Item) < 1 {
		return nil, nil
	}

	mdl, mErr := input.Unmarshal(res.Item)
	return mdl, mErr
}

func (h handlerImp) GetByIDs(ctx context.Context, input BaseModel, dbKeys []DBPSKeyValues) ([]BaseModel, error) {
	pages := Partition(len(dbKeys), 25)
	ch := make(chan baseModelsWithErr, len(pages))

	for page := range pages {
		go func(page IdxRange) {
			req := h.buildGetRequests(dbKeys[page.Low:page.High])
			h.loadPage(ctx, input, req, ch)
		}(page)
	}

	records := make([]BaseModel, 0, len(dbKeys))
	for {
		res, done := <-ch
		if res.Err != nil {
			return nil, res.Err
		}
		records = append(records, res.Records...)

		if done {
			break
		}
	}
	return records, nil
}

func (h handlerImp) GetRecordsWithScanFilter(ctx context.Context, input BaseModel, filters *AwsExpressionWrapper) ([]BaseModel, DBAttributeValues, error) {
	scanInput, err := filters.BuildScanInput()
	if err != nil {
		return nil, nil, err
	}

	res, getErr := h.ScanWithContext(ctx, scanInput)
	if getErr != nil {
		return nil, nil, getErr
	}

	items := make([]BaseModel, 0, len(res.Items))

	for _, item := range res.Items {
		mdl, mErr := input.Unmarshal(item)
		if mErr != nil {
			return nil, nil, mErr
		}
		items = append(items, mdl)
	}

	return items, res.LastEvaluatedKey, nil
}

func (h handlerImp) GetRecordsWithQueryFilter(ctx context.Context, input BaseModel, filters *AwsExpressionWrapper) ([]BaseModel, DBAttributeValues, error) {
	query, err := filters.BuildQueryInput()
	if err != nil {
		return nil, nil, err
	}

	res, getErr := h.QueryWithContext(ctx, query)
	if getErr != nil {
		return nil, nil, getErr
	}

	items := make([]BaseModel, 0, len(res.Items))

	for _, item := range res.Items {
		mdl, mErr := input.Unmarshal(item)
		if mErr != nil {
			return nil, nil, mErr
		}
		items = append(items, mdl)
	}

	return items, res.LastEvaluatedKey, nil
}

func (h handlerImp) prepareGetReq(name DynamoTableOrIndexName, keys DBPSKeyValues) (*dynamodb.GetItemInput, error) {
	if len(keys.GetPartitionKey()) < 1 {
		return nil, errors.New("invalid partition key")
	}

	dbKeys := h.config.TableInfo.DBPSKeyNames

	if keys, ok := h.config.Indexes[name]; ok {
		dbKeys = keys
	}

	expr := NewExpressionWrapper(h.config.TableInfo.TableName).
		WithPartitionKey(string(dbKeys.PartitionKey), string(keys.GetPartitionKey()))

	if dbKeys.SortKey != nil && keys.GetSortKey() != nil {
		expr.WithSortingKey(string(*dbKeys.SortKey), string(*keys.GetSortKey()))
	}

	return expr.BuildGetInput()
}

// buildGetRequests takes a list of ids and prepare list of BatchGetItemInput
func (h handlerImp) buildGetRequests(ids []DBPSKeyValues) *dynamodb.BatchGetItemInput {
	tabInfo := h.config.TableInfo
	dbKeys := tabInfo.DBPSKeyNames
	// create and accumulate the attributes for the input
	attributes := make([]map[string]*dynamodb.AttributeValue, 0)
	for _, id := range ids {
		expr := NewExpressionWrapper(h.config.TableInfo.TableName).
			WithPartitionKey(string(dbKeys.PartitionKey), string(id.GetPartitionKey()))

		if dbKeys.SortKey != nil && id.GetSortKey() != nil {
			expr.WithSortingKey(string(*dbKeys.SortKey), string(*id.GetSortKey()))
		}

		attribute, err := expr.CreateQueryKeys()
		if err != nil {
			// ignore wrong Ids
			continue
		}
		attributes = append(attributes, attribute)
	}
	// create and return the Batch get request
	return &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			tabInfo.TableName: {
				Keys: attributes,
			},
		},
	}
}

func (h handlerImp) loadPage(ctx context.Context, model BaseModel, req *dynamodb.BatchGetItemInput, ch chan baseModelsWithErr) {
	records := make([]BaseModel, 0)
	// deserialize received output
	acc := func(res *dynamodb.BatchGetItemOutput) error {
		for _, item := range res.Responses[h.config.TableInfo.TableName] {
			mdl, err := model.Unmarshal(item)
			if err != nil {
				return err
			}
			records = append(records, mdl)
		}
		return nil
	}

	var load func(req *dynamodb.BatchGetItemInput) error

	load = func(req *dynamodb.BatchGetItemInput) error {
		var res *dynamodb.BatchGetItemOutput
		var err error

		if req != nil {
			res, err = h.BatchGetItemWithContext(ctx, req)
			if err != nil {
				return err
			}
		}

		if res != nil && len(res.Responses) > 0 {
			if err := acc(res); err != nil {
				return err
			}
		}
		if len(res.UnprocessedKeys) > 0 {
			return load(&dynamodb.BatchGetItemInput{
				RequestItems: res.UnprocessedKeys,
			})
		}
		return nil
	}

	err := load(req)
	ch <- baseModelsWithErr{
		Records: records,
		Err:     err,
	}
}
