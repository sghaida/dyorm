package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestBaseModel_Marshal(t *testing.T) {
	mdl := TestBaseModel{
		Name: "test",
	}
	ser, err := mdl.Marshal()
	assert.NoError(t, err)
	assert.True(t, len(ser) > 0)
}

func TestBaseModelStub_Unmarshal(t *testing.T) {
	dynamoMap := DBMap{
		"name": &dynamodb.AttributeValue{
			S: aws.String("golang"),
		},
		"Age": &dynamodb.AttributeValue{
			N: aws.String("18"),
		},
	}

	mdl := TestBaseModel{}
	res, err := mdl.Unmarshal(dynamoMap)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	assert.Equal(t, "golang", res.(TestBaseModel).Name)
	assert.Equal(t, 18, res.(TestBaseModel).Age)
}

func TestTestBaseModel_GetModelType(t *testing.T) {
	mdl := TestBaseModel{}
	expected := DBModelName("TestBaseModel")
	assert.Equal(t, expected, mdl.GetModelType())
}
