package awsDynamo_test

/*
   @author: vikram
*/

import (
	logger "github.com/astaxie/beego"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/controller/reqresp"
	apiError "payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/error"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/services/awsDynamo"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/status"
	"strings"
	"testing"
)

func init() {
	logger.SetLogFuncCall(true)

}

func TestDynamoDB_InsertItem(t *testing.T) {

	dynamoDB := &awsDynamo.Client{Client: getDynamoDBClient()}

	tableName := "test_InsertItem"
	createTable(dynamoDB, tableName, "partitionKey", "")

	testCases := []struct {
		tableName string
		item      map[string]interface{}
		expected  string
	}{
		// TODO Write test cases for below comments
		//{"test_InsertItem", map[string]interface{} {"abcd": channelError }, "item cannot be marshaled into required DynamoDB format"},
		{"test_InsertItem_", map[string]interface{}{"AttributeName": "AttributeValue"}, "ResourceNotFoundException: Requested resource not found. Table: test_InsertItem_ may not exist"},
		{tableName, map[string]interface{}{"column2": "pqr"}, "ValidationException: One or more parameter values were invalid: Missing the key partitionKey in the item"},
		// ErrCodeProvisionedThroughputExceededException
		// ErrCodeInternalServerError
		{tableName, map[string]interface{}{"partitionKey": "M001", "column2": "abc", "column3": "X3yt87h"}, ""},
	}

	for _, testCase := range testCases {
		var errInfo string
		if errorResponse := dynamoDB.InsertItem(testCase.tableName, testCase.item); errorResponse != nil {
			errInfo = errorResponse.APIResponse.(*apiError.APIError).ErrInfo
		}

		assert.Equalf(t, testCase.expected, errInfo, "Expected: %v, Got: %v", testCase.expected, errInfo)
	}

	deleteTable(dynamoDB, tableName)
}

func TestDynamoDB_UpdateItem(t *testing.T) {

	tableName := "test-update-item"
	dynamoDB := &awsDynamo.Client{Client: getDynamoDBClient()}

	createTable(dynamoDB, tableName, "partitionKey", "")
	dynamoDB.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      map[string]*dynamodb.AttributeValue{"partitionKey": {S: aws.String("pqrd")}, "abc": {S: aws.String("pqr")}},
	})
	dynamoDB.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      map[string]*dynamodb.AttributeValue{"partitionKey": {S: aws.String("pqrde")}, "abc": {S: aws.String("123")}},
	})
	responseErr1 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"ValidationException: One or more parameter values were invalid: Cannot update attribute partitionKey."+
				" This attribute is part of the key"),
		HTTPStatusCode: status.UnProcessableEntity,
	}
	responseErr2 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"table: test doesn't exist"),
		HTTPStatusCode: status.UnProcessableEntity,
	}
	responseErr3 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"ValidationException: The provided key element does not match the schema"),
		HTTPStatusCode: status.UnProcessableEntity,
	}
	responseErr4 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"no such item found, error: The conditional request failed"),
		HTTPStatusCode: status.UnProcessableEntity,
	}
	responseSuccess1 := &reqresp.Response{
		APIResponse: map[string]interface{}{
			"partitionKey": "pqrd",
			"abc":          "xyz",
			"def":          float64(123),
		},
		HTTPStatusCode: status.OK,
	}
	responseSuccess2 := &reqresp.Response{
		APIResponse: map[string]interface{}{
			"partitionKey": "pqrd",
			"abc":          "xyz",
			"def":          nil,
		},
		HTTPStatusCode: status.OK,
	}
	responseSuccess3 := &reqresp.Response{
		APIResponse: map[string]interface{}{
			"partitionKey": "pqrd",
			"abc":          float64(456),
			"def":          "abc",
		},
		HTTPStatusCode: status.OK,
	}

	testTable := []struct {
		tableName  string
		key        map[string]interface{}
		item       map[string]interface{}
		conditions []reqresp.Condition
		want       *reqresp.Response
	}{
		{"test", map[string]interface{}{"abc": "pqr"}, map[string]interface{}{"abcd": "pqrd"},
			nil, responseErr2},

		{tableName, map[string]interface{}{"abc": "pqr"}, map[string]interface{}{"abcd": "pqrd"},
			nil, responseErr3},

		{tableName, map[string]interface{}{"partitionKey": "pqr"}, map[string]interface{}{"abc": "pqrd"},
			nil, responseErr4},

		{tableName, map[string]interface{}{"partitionKey": "pqr"}, map[string]interface{}{"abc": "pqrd"},
			[]reqresp.Condition{{Key: "partitionKey", Value: "abc", Operation: "="}}, responseErr4},

		{tableName, map[string]interface{}{"partitionKey": "pqrd"}, map[string]interface{}{"abc": "pqrd"},
			[]reqresp.Condition{{Key: "abc", Value: "abc", Operation: "="}}, responseErr4},

		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			map[string]interface{}{"abc": "xyz", "def": 123}, nil, responseSuccess1},

		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			map[string]interface{}{"partitionKey": "abcd", "abc": "xyz", "def": ""}, nil, responseErr1},

		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			map[string]interface{}{"abc": "xyz", "def": ""}, nil, responseSuccess2},

		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			map[string]interface{}{"abc": 456, "def": "abc"}, nil, responseSuccess3},
	}

	for _, testData := range testTable {
		response := dynamoDB.UpdateItem(testData.tableName, testData.item, testData.key, testData.conditions)
		assert.Equal(t, testData.want, response)
	}

	deleteTable(dynamoDB, tableName)
}

func TestDynamoDB_DeleteItem(t *testing.T) {

	tableName := "test-delete-item"
	dynamoDB := &awsDynamo.Client{Client: getDynamoDBClient()}
	createTable(dynamoDB, tableName, "partitionKey", "")
	dynamoDB.InsertItem(tableName, map[string]interface{}{"partitionKey": "pqrd", "abc": "pqr"})

	responseErr1 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"table: test doesn't exist"),
		HTTPStatusCode: status.UnProcessableEntity,
	}

	responseErr2 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"ValidationException: The provided key element does not match the schema"),
		HTTPStatusCode: status.UnProcessableEntity,
	}
	/*responseErr3 := &reqresp.Response{
		APIResponse: apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity),
			"ValidationException: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"),
		HTTPStatusCode: status.UnProcessableEntity,
	}*/

	responseSuccess1 := &reqresp.Response{
		HTTPStatusCode: status.NoContent,
	}

	testTable := []struct {
		tableName  string
		primaryKey map[string]interface{}
		conditions []reqresp.Condition
		want       *reqresp.Response
	}{
		{"test", map[string]interface{}{"partitionKey": "pqr"}, nil, responseErr1},
		{tableName, map[string]interface{}{"abc": "pqr"}, nil, responseErr2},
		{tableName, map[string]interface{}{"partitionKey": "pqr"}, nil, responseSuccess1},
		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			[]reqresp.Condition{{Key: "abc", Value: "abc", Operation: "="}}, responseSuccess1},
		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			[]reqresp.Condition{{Key: "abc", Value: "pqr", Operation: "<>"}}, responseSuccess1},
		{tableName, map[string]interface{}{"partitionKey": "pqrd"},
			[]reqresp.Condition{{Key: "abc", Value: "pqr", Operation: "="}}, responseSuccess1},
		/*{"test_deleteitem", map[string]*dynamodb.AttributeValue{"abc": {S: aws.String("pqr")}}, errors.New("missing one of the primaryKey, invalid " +
			"request")},
		{"test_deleteitem", map[string]*dynamodb.AttributeValue{"partitionKey": {S: aws.String("pqr")}}, errors.New("the conditional request " +
			"failed, ConditionalCheckFailedException")},
		{"test_deleteitem", map[string]*dynamodb.AttributeValue{"partitionKey": {S: aws.String("pqrd")}}, nil},
		*/}
	for _, testData := range testTable {
		response := dynamoDB.DeleteItem(testData.tableName, testData.primaryKey, testData.conditions)
		assert.Equal(t, testData.want, response)

	}

	deleteTable(dynamoDB, tableName)
}

/*func TestDynamoDB_BatchWriteItem(t *testing.T) {
	dynamoDB := &awsDynamo.DynamoDB{Client: getDynamoDBClient()}

	testTable := []struct {
		tableName string
		itemData  map[string]*dynamodb.AttributeValue
		want      error
	}{
		{"", nil, errors.New("tableName can't be empty")},
		{"test_batchwriteitem", nil, errors.New("inputData can't be nil or empty")},
		{"test_batchwriteitem", map[string]*dynamodb.AttributeValue{}, errors.New("inputData can't be nil or empty")},
		{"test_batchwriteitem", map[string]*dynamodb.AttributeValue{"abcd": {S: aws.String("pqrd")}}, errors.New("table: test_putitem doesn't exist")},
	}

	for _, testData := range testTable {
		err := dynamoDB.PutItem(testData.tableName, testData.itemData)
		assert.Equalf(t, testData.want, err, "want: %v, Got: %v", testData.want, err)
	}

	tableName := "test_batchwriteitem"
	createTable(dynamoDB, tableName, "partitionKey", "")
}*/

func getDynamoDBClient() *dynamodb.DynamoDB {

	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	}))
	dynamoDBClient := dynamodb.New(awsSession)
	return dynamoDBClient
}

/*func getBatchItemsData() []map[string]string {
	var batchItemData = []map[string]string{}
	for i := 0; i < 5; i++ {
		itemData := map[string]string{
			"operation_date": "2018-06-13 15:53:17.1" + strconv.Itoa(i),
			"partitionKey":   "63b65351-a51e-4ef0-8fa2-3119011107" + strconv.Itoa(i%2),
		}
		batchItemData = append(batchItemData, itemData)
	}
	return batchItemData
}*/

func createTable(dynamoDB *awsDynamo.Client, tableName string, hashKey string, sortKey string) {
	if strings.TrimSpace(sortKey) == "" {
		params := &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(hashKey),
					AttributeType: aws.String("S"),
				},
			},

			KeySchema: []*dynamodb.KeySchemaElement{

				{
					AttributeName: aws.String(hashKey),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		}
		dynamoDB.Client.CreateTable(params)
		dynamoDB.Client.WaitUntilTableExists(&dynamodb.DescribeTableInput{TableName: aws.String(tableName)})

	} else {
		params := &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{

				{
					AttributeName: aws.String(sortKey),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String(hashKey),
					AttributeType: aws.String("S"),
				},
			},

			KeySchema: []*dynamodb.KeySchemaElement{

				{
					AttributeName: aws.String(hashKey),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String(sortKey),
					KeyType:       aws.String("RANGE"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		}
		dynamoDB.Client.CreateTable(params)
		dynamoDB.Client.WaitUntilTableExists(&dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	}
}

func deleteTable(dynamoDB *awsDynamo.Client, tableName string) {

	deleteTableParam := dynamodb.DeleteTableInput{TableName: aws.String(tableName)}
	_, err := dynamoDB.Client.DeleteTable(&deleteTableParam)
	if err != nil {
		logger.Error("[deleteTable] Error occurred while deleting table: ", tableName)
	}
}
