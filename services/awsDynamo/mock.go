package awsDynamo

/*
   @author: vedant
*/

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/controller/reqresp"
)

// MockClient for mocking database DAO layer functions
type MockClient struct {
	MockInsertItem func(tableName string, item map[string]interface{}) *reqresp.Response
	MockUpdateItem func(tableName string, item map[string]interface{}, key map[string]interface{},
		conditions []reqresp.Condition) *reqresp.Response
	MockDeleteItem func(tableName string, key map[string]interface{},
		conditions []reqresp.Condition) *reqresp.Response
	MockBatchOperations func(tableName string, items []map[string]interface{}, operationType string) *reqresp.Response
}

// InsertItem for mock
func (mock *MockClient) InsertItem(tableName string, item map[string]interface{}) *reqresp.Response {
	return mock.MockInsertItem(tableName, item)
}

// UpdateItem for mock
func (mock *MockClient) UpdateItem(tableName string, item map[string]interface{}, key map[string]interface{},
	conditions []reqresp.Condition) *reqresp.Response {
	return mock.MockUpdateItem(tableName, item, key, conditions)
}

// DeleteItem for mock
func (mock *MockClient) DeleteItem(tableName string, key map[string]interface{},
	conditions []reqresp.Condition) *reqresp.Response {
	return mock.MockDeleteItem(tableName, key, conditions)
}

// BatchWriteItem for mock
func (mock *MockClient) BatchWriteItem(tableName string, items []map[string]*dynamodb.AttributeValue) {
	panic("implement me")
}

// BatchOperations for mock
func (mock *MockClient) BatchOperations(tableName string, keys []map[string]interface{}, operationType string) *reqresp.Response {
	return mock.MockBatchOperations(tableName, keys, operationType)
}
