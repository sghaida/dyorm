package dynamodb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	dynamoSDK "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"

	"github.com/sghaida/dyorm"
)

const (
	CreatedAt string = "CreatedAt"
	UpdatedAt string = "UpdatedAt"
)

func Test_BuildExpression(t *testing.T) {
	t.Run("expression with and", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndCondition("partitionID", "1234", 10).
			AndCondition("UserID", "abcd", dynamodb.EQUAL).
			OrCondition("sortID", "abc", dynamodb.EQUAL).
			WithPartitionKey("partitionID", "1234").
			WithSortingKey("UserID", "abcd").
			WithUpdateField(UpdatedAt, time.Now().Unix()).
			WithUpdateField(CreatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err != nil {
			t.Errorf("expect building input from expression to be successful, got %v", err)
		}
	})

	t.Run("expression with no condition", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithPartitionKey("partitionID", "1234").
			WithSortingKey("UserID", "abcd").
			WithUpdateField(UpdatedAt, time.Now().Unix()).
			WithUpdateField(CreatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err != nil {
			t.Errorf("expect building input from expression to be successful, got %v", err)
		}
	})

	t.Run("expression with or", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			OrCondition("partitionID", "1234", dynamodb.EQUAL).
			OrCondition("sortID", "abc", dynamodb.EQUAL).
			WithPartitionKey("partitionID", "1234").
			WithSortingKey("UserID", "abcd").
			WithUpdateField(UpdatedAt, time.Now().Unix()).
			WithUpdateField(CreatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err != nil {
			t.Errorf("expect building input from expression to be successful, got %v", err)
		}
	})

	t.Run("expression with less than", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			OrCondition("partitionID", 1234, dynamodb.LE).
			OrCondition("sortID", 123, dynamodb.LT).
			WithPartitionKey("partitionID", "1234").
			WithUpdateField(UpdatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err != nil {
			t.Errorf("expect building input from expression to be successful, got %v", err)
		}
	})

	t.Run("expression with greater than", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			OrCondition("partitionID", 123, dynamodb.GT).
			OrCondition("sortID", 123, dynamodb.GE).
			WithPartitionKey("partitionID", "1234").
			WithUpdateField(UpdatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err != nil {
			t.Errorf("expect building input from expression to be successful, got %v", err)
		}
	})

	t.Run("expression without keys", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			OrCondition("partitionID", 123, dynamodb.GT).
			OrCondition("sortID", 123, dynamodb.GE).
			WithUpdateField(UpdatedAt, time.Now().Unix())

		_, err := expr.BuildUpdateInput()
		if err == nil {
			t.Error("expect building input error, got nil")
		}
	})

	t.Run("expression with update value", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			OrCondition("partitionID", 123, dynamodb.GT).
			OrCondition("sortID", 123, dynamodb.GE)

		_, err := expr.BuildUpdateInput()
		if err == nil {
			t.Error("expect building input error, got nil")
		}
	})

	t.Run("expression with get value", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE)

		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})

	t.Run("expression with get value with condition", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE).
			WithCondition("abc", 123, dynamodb.GE)

		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})

	t.Run("expression with get value with condition and scan index forward", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE).
			WithCondition("abc", 123, dynamodb.GE).
			WithScanIndexForward(true)

		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})

	t.Run("expression with get value with condition and with limit", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE).
			WithCondition("abc", 123, dynamodb.GE).
			WithLimit(10)

		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})

	t.Run("expression with get value with condition and exclusive start key", func(t *testing.T) {
		expectedName := "golang"
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE).
			WithCondition("abc", 123, dynamodb.GE).
			WithExlusiveStartingKey(
				map[string]*dynamoSDK.AttributeValue{
					"name": {
						S: &expectedName,
					},
				},
			)

		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})

	t.Run("wrong expression with get value", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndKeyCondition("partitionID", 123, dynamodb.LE).
			AndKeyCondition("sortID", 123, dynamodb.LT).
			AndKeyCondition("xyz", 123, dynamodb.GT).
			AndKeyCondition("abc", 123, 10)

		_, err := expr.BuildQueryInput()
		if err == nil {
			t.Error("expect building input error, got nil")
		}
	})

	t.Run("date between expression", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndKeyCondition("partitionID", 123, dynamodb.LE).
			AndKeyCondition("sortID", 123, dynamodb.LT).
			AndCondition(
				"abc",
				dynamodb.FromToDate{FromDate: 0, ToDate: 1586190435},
				dynamodb.BETWEEN,
			)
		_, err := expr.BuildQueryInput()
		if err == nil {
			t.Error("expect building input error, got nil")
		}
	})

	t.Run("any thing between expression with single field projection", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndKeyCondition("partitionID", 123, dynamodb.LE).
			WithProjection("a").
			WithIndexName("bla-bla-bla")
		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building input to succeed, got %v", err)
		}
	})

	t.Run("any thing between expression with multi field projection", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndKeyCondition("partitionID", 123, dynamodb.LE).
			WithProjection("a", "b", "c").
			WithIndexName("bla-bla-bla")
		_, err := expr.BuildQueryInput()
		if err != nil {
			t.Errorf("expect building input to succeed, got %v", err)
		}
	})

	t.Run("date less than expression", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			AndKeyCondition("partitionID", 123, dynamodb.LE).
			AndKeyCondition("sortID", 123, dynamodb.LT).
			AndCondition("abc", dynamodb.FromToDate{FromDate: 0, ToDate: 1586190435}, dynamodb.LT)
		_, err := expr.BuildQueryInput()
		if err == nil {
			t.Error("expect building input error, got nil")
		}
	})

	t.Run("get item group", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("tablename").
				WithPartitionKey("partKeyName", "partKeyVal").
				WithSortingKey("sortkeyName", "sortKeyVal")

			req, err := expr.BuildGetInput()
			assert.NoError(t, err)
			assert.NotNil(t, req)
			if req != nil {
				assert.NotEmpty(t, *req)
			}
		})

		t.Run("without a table-name", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("").
				WithPartitionKey("partKeyName", "partKeyVal").
				WithSortingKey("sortkeyName", "sortKeyVal")

			req, err := expr.BuildGetInput()
			assert.Error(t, err)
			assert.Nil(t, req)
		})

		t.Run("without a partition key", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("tablename")
			req, err := expr.BuildGetInput()
			assert.Error(t, err)
			assert.Nil(t, req)
		})
	})

	t.Run("delete item group", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("request-test").
				WithPartitionKey("partitionID", "1234")

			_, err := expr.BuildDeleteInput()
			if err != nil {
				t.Errorf("expect building input from expression to be successful, got %v", err)
			}
		})
		t.Run("successfully with part and sort key", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("request-test").
				WithPartitionKey("partitionID", "1234").
				WithSortingKey("sortingKey", "sortID")

			_, err := expr.BuildDeleteInput()
			if err != nil {
				t.Errorf("expect building input from expression to be successful, got %v", err)
			}
		})
		t.Run("success with condition", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("request-test").
				AndCondition("sortID", 123, dynamodb.GE).
				WithPartitionKey("partitionID", "1234")

			_, err := expr.BuildDeleteInput()
			if err != nil {
				t.Errorf("expect building input from expression to be successful, got %v", err)
			}
		})

		t.Run("without a table-name", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("").
				WithPartitionKey("partKeyName", "partKeyVal").
				WithSortingKey("sortkeyName", "sortKeyVal")

			req, err := expr.BuildDeleteInput()
			assert.Error(t, err)
			assert.Nil(t, req)
		})

		t.Run("without a partition key", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("tablename")
			req, err := expr.BuildDeleteInput()
			assert.Error(t, err)
			assert.Nil(t, req)
		})

		t.Run("with empty partition key value", func(t *testing.T) {
			expr := dynamodb.NewExpressionWrapper("tablename").
				WithPartitionKey("partitionKey", "")
			req, err := expr.BuildDeleteInput()
			assert.Error(t, err)
			assert.Nil(t, req)
		})
	})
	t.Run("build scan input successfully", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("request-test").
			WithKeyCondition("partitionID", 123, dynamodb.EQUAL).
			AndKeyCondition("sortID", 123, dynamodb.GE).
			WithCondition("abc", 123, dynamodb.GE).
			WithLastEvaluatedKey("pKeyName", "pKeyVal", aws.String("sKey"), aws.String("sKeyVal")).
			WithLimit(int64(5))

		_, err := expr.BuildScanInput()
		if err != nil {
			t.Errorf("expect building to succeed, got %v", err)
		}
	})
	t.Run("build scan-input without a table-name", func(t *testing.T) {
		expr := dynamodb.NewExpressionWrapper("").
			WithPartitionKey("partKeyName", "partKeyVal").
			WithSortingKey("sortkeyName", "sortKeyVal")

		req, err := expr.BuildScanInput()
		assert.Error(t, err)
		assert.Nil(t, req)
	})

}
