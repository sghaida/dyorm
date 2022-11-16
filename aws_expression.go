package dynamodb

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Operator DynamoDB operators for string and numbers
type Operator int

const (
	// EQUAL equality operator
	EQUAL Operator = iota + 1
	// LT less than operator
	LT
	// LE less or equal
	LE
	// GT greater than
	GT
	// GE greater or equal
	GE
	// BETWEEN upper and lower
	BETWEEN
)

// FromToDate which to be used in constructing the between operations for date
type FromToDate struct {
	FromDate uint64
	ToDate   uint64
}

// AwsExpressionWrapper ...
type AwsExpressionWrapper struct {
	updateExpression    expression.UpdateBuilder
	conditionExpression expression.ConditionBuilder
	keyCondition        expression.KeyConditionBuilder
	projection          expression.ProjectionBuilder
	partitionKeyValue   *dynamodb.AttributeValue
	sortKeyValue        *dynamodb.AttributeValue
	exclusiveStartKey   map[string]*dynamodb.AttributeValue
	scanIndexForward    *bool
	partitionKeyName    string
	sortKeyName         string
	dynamoDBTable       string
	dynamoDBIndex       string
	limit               *int64
}

// NewExpressionWrapper creates new expression wrapper
func NewExpressionWrapper(tableName string) *AwsExpressionWrapper {
	return &AwsExpressionWrapper{
		updateExpression:    expression.UpdateBuilder{},
		conditionExpression: expression.ConditionBuilder{},
		keyCondition:        expression.KeyConditionBuilder{},
		projection:          expression.ProjectionBuilder{},
		dynamoDBTable:       tableName,
	}
}

// WithIndexName set the index name, this is used in case get is reading an index
func (expr *AwsExpressionWrapper) WithIndexName(indexName string) *AwsExpressionWrapper {
	expr.dynamoDBIndex = indexName
	return expr
}

// WithProjection add projected fields to retrieve
func (expr *AwsExpressionWrapper) WithProjection(fields ...string) *AwsExpressionWrapper {
	names := make([]expression.NameBuilder, 0)
	for _, field := range fields {
		if field != "" {
			names = append(names, expression.Name(field))
		}
	}
	if len(names) == 1 {
		expr.projection = expression.NamesList(names[0])
	}
	if len(names) > 1 {
		expr.projection = expression.NamesList(names[0], names[1:]...)
	}
	return expr
}

// WithUpdateField sets update expression value for a specific field name
func (expr *AwsExpressionWrapper) WithUpdateField(name string, value interface{}) *AwsExpressionWrapper {
	if reflect.DeepEqual(expr.updateExpression, expression.UpdateBuilder{}) {
		expr.updateExpression = expression.Set(
			expression.Name(name),
			expression.Value(value),
		)
		return expr
	}
	expr.updateExpression.Set(
		expression.Name(name),
		expression.Value(value),
	)
	return expr
}

// WithLimit sets the maximum number of items to evaluate
func (expr *AwsExpressionWrapper) WithLimit(limit int64) *AwsExpressionWrapper {
	expr.limit = aws.Int64(limit)
	return expr
}

// WithCondition sets the initial condition
func (expr *AwsExpressionWrapper) WithCondition(
	name string, value interface{}, operator Operator,
) *AwsExpressionWrapper {
	expr.conditionExpression = createCondition(name, value, operator)
	return expr
}

// AndCondition adds to the initial condition an AND condition if exists or create new condition
func (expr *AwsExpressionWrapper) AndCondition(
	name string, value interface{}, operator Operator,
) *AwsExpressionWrapper {

	if reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		expr.WithCondition(name, value, operator)
		return expr
	}
	condition := createCondition(name, value, operator)
	newConditionExpr := expr.conditionExpression.And(condition)
	expr.conditionExpression = newConditionExpr
	return expr
}

// OrCondition adds to the initial condition an OR condition if exists or create new condition
func (expr *AwsExpressionWrapper) OrCondition(
	name string, value interface{}, operator Operator,
) *AwsExpressionWrapper {

	if reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		expr.WithCondition(name, value, operator)
		return expr
	}
	condition := createCondition(name, value, operator)
	newConditionExpr := expr.conditionExpression.Or(condition)
	expr.conditionExpression = newConditionExpr
	return expr
}

// WithKeyCondition sets the initial key condition
// first key should always be using EQUAL operator as it represents the partition key
func (expr *AwsExpressionWrapper) WithKeyCondition(
	name string, value interface{}, operator Operator,
) *AwsExpressionWrapper {
	expr.keyCondition = createKeyCondition(name, value, operator)
	return expr
}

// AndKeyCondition adds to the initial condition an AND condition if exists or create composite condition with and
func (expr *AwsExpressionWrapper) AndKeyCondition(
	name string, value interface{}, operator Operator,
) *AwsExpressionWrapper {

	if reflect.DeepEqual(expr.keyCondition, expression.KeyConditionBuilder{}) {
		expr.WithKeyCondition(name, value, operator)
		return expr
	}
	cond1 := expr.keyCondition
	cond2 := createKeyCondition(name, value, operator)
	expr.keyCondition = expression.KeyAnd(cond1, cond2)
	return expr
}

// WithPartitionKey adds partition key
func (expr *AwsExpressionWrapper) WithPartitionKey(pKey string, pValue string) *AwsExpressionWrapper {
	expr.partitionKeyName = pKey
	if len(pValue) > 0 {
		expr.partitionKeyValue = &dynamodb.AttributeValue{S: aws.String(pValue)}
	}
	return expr
}

// WithSortingKey adds sorting key if available
func (expr *AwsExpressionWrapper) WithSortingKey(sKey string, sValue string) *AwsExpressionWrapper {
	expr.sortKeyName = sKey
	if len(sValue) > 0 {
		expr.sortKeyValue = &dynamodb.AttributeValue{S: aws.String(sValue)}
	}
	return expr
}

// WithLastEvaluatedKey defines the last evaluated key
func (expr *AwsExpressionWrapper) WithLastEvaluatedKey(pKeyName, pKeyVal string, sKeyName, sKeyVal *string) *AwsExpressionWrapper {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue)
	lastEvaluatedKey[pKeyName] = &dynamodb.AttributeValue{
		S: aws.String(pKeyVal),
	}

	if sKeyName != nil && sKeyVal != nil {
		lastEvaluatedKey[*sKeyName] = &dynamodb.AttributeValue{
			S: aws.String(*sKeyVal),
		}
	}

	expr.exclusiveStartKey = lastEvaluatedKey

	return expr
}

// WithScanIndexForward DESC should be FALSE and ASC should be TRUE
// it is used to return th values sorted on the basis of sorting key
func (expr *AwsExpressionWrapper) WithScanIndexForward(asc bool) *AwsExpressionWrapper {
	expr.scanIndexForward = aws.Bool(asc)
	return expr
}

// WithExlusiveStartingKey to return Starting key of next page, key will be in form of structure
func (expr *AwsExpressionWrapper) WithExlusiveStartingKey(lastEvaluatedKey map[string]*dynamodb.AttributeValue) *AwsExpressionWrapper {
	expr.exclusiveStartKey = lastEvaluatedKey
	return expr
}

// BuildUpdateInput build the update input out of the update expression
func (expr *AwsExpressionWrapper) BuildUpdateInput() (*dynamodb.UpdateItemInput, error) {
	if reflect.DeepEqual(expr.updateExpression, expression.UpdateBuilder{}) {
		return nil, errors.New("their is nothing set to be updated, please use WithUpdateField")
	}

	keys, keyErr := expr.CreateQueryKeys()
	if keyErr != nil {
		return nil, keyErr
	}

	builder := expression.NewBuilder().WithUpdate(expr.updateExpression)

	awsExpressionBuilder, err := builder.Build()
	return &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  awsExpressionBuilder.Names(),
		ExpressionAttributeValues: awsExpressionBuilder.Values(),
		UpdateExpression:          awsExpressionBuilder.Update(),
		Key:                       keys,
		TableName:                 aws.String(expr.dynamoDBTable),
	}, err
}

// BuildQueryInput builds the expression and return the input to be used for the get
func (expr *AwsExpressionWrapper) BuildQueryInput() (*dynamodb.QueryInput, error) {
	builder := expression.NewBuilder()
	// check for available condition
	if !reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		builder = builder.WithFilter(expr.conditionExpression)
	}

	if !reflect.DeepEqual(expr.projection, expression.ProjectionBuilder{}) {
		builder = builder.WithProjection(expr.projection)
	}

	awsExpressionBuilder, err := builder.
		WithKeyCondition(expr.keyCondition).
		Build()

	if err != nil {
		return nil, err
	}

	input := dynamodb.QueryInput{
		ExpressionAttributeNames:  awsExpressionBuilder.Names(),
		ExpressionAttributeValues: awsExpressionBuilder.Values(),
		KeyConditionExpression:    awsExpressionBuilder.KeyCondition(),
		TableName:                 aws.String(expr.dynamoDBTable),
	}

	// if there is a projection already defined add the projection
	if awsExpressionBuilder.Projection() != nil {
		input.ProjectionExpression = awsExpressionBuilder.Projection()
	}

	// if there is a filter already defined add the filter
	if !reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		input.FilterExpression = awsExpressionBuilder.Filter()
	}
	// check if index is available
	if len(expr.dynamoDBIndex) > 0 {
		input.IndexName = aws.String(expr.dynamoDBIndex)
	}

	if expr.scanIndexForward != nil {
		input.ScanIndexForward = expr.scanIndexForward
	}

	if expr.limit != nil && *expr.limit >= 1 {
		input.Limit = expr.limit
	}

	if len(expr.exclusiveStartKey) > 0 {
		input.ExclusiveStartKey = expr.exclusiveStartKey
	}

	return &input, nil
}

// BuildScanInput create scan query expression
func (expr *AwsExpressionWrapper) BuildScanInput() (*dynamodb.ScanInput, error) {
	if len(expr.dynamoDBTable) == 0 {
		return nil, errors.New("missing table-name")
	}
	input := dynamodb.ScanInput{
		TableName: aws.String(expr.dynamoDBTable),
	}

	builder := expression.NewBuilder()
	// check for available condition
	if !reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		builder = builder.WithFilter(expr.conditionExpression)
		awsExpressionBuilder, _ := builder.Build()

		input = dynamodb.ScanInput{
			ExpressionAttributeNames:  awsExpressionBuilder.Names(),
			ExpressionAttributeValues: awsExpressionBuilder.Values(),
			FilterExpression:          awsExpressionBuilder.Filter(),
			TableName:                 aws.String(expr.dynamoDBTable),
		}
	}

	if !reflect.DeepEqual(expr.keyCondition, expression.ConditionBuilder{}) {
		builder = builder.WithKeyCondition(expr.keyCondition)
		awsExpressionBuilder, _ := builder.Build()

		input = dynamodb.ScanInput{
			ExpressionAttributeNames:  awsExpressionBuilder.Names(),
			ExpressionAttributeValues: awsExpressionBuilder.Values(),
			FilterExpression:          awsExpressionBuilder.KeyCondition(),
			TableName:                 aws.String(expr.dynamoDBTable),
		}
	}

	if expr.limit != nil && *expr.limit >= 1 {
		input.Limit = expr.limit
	}

	if len(expr.exclusiveStartKey) > 0 {
		input.ExclusiveStartKey = expr.exclusiveStartKey
	}

	return &input, nil
}

// BuildGetInput build get input expression
func (expr *AwsExpressionWrapper) BuildGetInput() (*dynamodb.GetItemInput, error) {
	if len(expr.dynamoDBTable) < 1 {
		return nil, errors.New("missing table name")
	}

	keys, err := expr.CreateQueryKeys()
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemInput{
		TableName: aws.String(expr.dynamoDBTable),
		Key:       keys,
	}, nil
}

// BuildDeleteInput build delete input
func (expr *AwsExpressionWrapper) BuildDeleteInput() (*dynamodb.DeleteItemInput, error) {
	if len(expr.dynamoDBTable) < 1 {
		return nil, errors.New("missing table name")
	}

	keys, keyErr := expr.CreateQueryKeys()
	if keyErr != nil {
		return nil, keyErr
	}

	input := dynamodb.DeleteItemInput{
		Key:       keys,
		TableName: aws.String(expr.dynamoDBTable),
	}

	if !reflect.DeepEqual(expr.conditionExpression, expression.ConditionBuilder{}) {
		awsExpressionBuilder, err := expression.NewBuilder().
			WithCondition(expr.conditionExpression).
			Build()

		if err != nil {
			return &input, err
		}

		input.ExpressionAttributeNames = awsExpressionBuilder.Names()
		input.ExpressionAttributeValues = awsExpressionBuilder.Values()
		input.ConditionExpression = awsExpressionBuilder.Condition()
	}

	return &input, nil
}

// CreateQueryKeys creates a query keys
func (expr *AwsExpressionWrapper) CreateQueryKeys() (map[string]*dynamodb.AttributeValue, error) {
	if len(expr.partitionKeyName) < 1 || expr.partitionKeyValue == nil {
		return nil, errors.New("missing partition key")
	}

	attributeValues := map[string]*dynamodb.AttributeValue{
		expr.partitionKeyName: expr.partitionKeyValue,
	}

	if len(expr.sortKeyName) > 0 {
		attributeValues[expr.sortKeyName] = expr.sortKeyValue
	}

	return attributeValues, nil
}

// createCondition creates the condition builder
func createCondition(name string, value interface{}, operator Operator) expression.ConditionBuilder {
	// check if the interface can be cast to FromToDate as the operation will be different
	switch obj := value.(type) {
	case FromToDate:
		switch operator {
		case BETWEEN:
			return expression.Name(name).Between(
				expression.Value(obj.FromDate),
				expression.Value(obj.ToDate),
			)
		default:
			// failsafe as the minimum value is going to be 0 for epoch
			return expression.Name(name).GreaterThanEqual(expression.Value(obj.FromDate))
		}
	}

	switch operator {
	case EQUAL:
		return expression.Name(name).Equal(expression.Value(value))
	case LT:
		return expression.Name(name).LessThan(expression.Value(value))
	case LE:
		return expression.Name(name).LessThanEqual(expression.Value(value))
	case GT:
		return expression.Name(name).GreaterThan(expression.Value(value))
	case GE:
		return expression.Name(name).GreaterThanEqual(expression.Value(value))
	default:
		return expression.Name(name).Equal(expression.Value(value))
	}
}

// createKeyCondition creates the condition builder
func createKeyCondition(name string, value interface{}, operator Operator) expression.KeyConditionBuilder {
	switch operator {
	case EQUAL:
		return expression.Key(name).Equal(expression.Value(value))
	case LT:
		return expression.Key(name).LessThan(expression.Value(value))
	case LE:
		return expression.Key(name).LessThanEqual(expression.Value(value))
	case GT:
		return expression.Key(name).GreaterThan(expression.Value(value))
	case GE:
		return expression.Key(name).GreaterThanEqual(expression.Value(value))
	default:
		return expression.Key(name).Equal(expression.Value(value))
	}
}
