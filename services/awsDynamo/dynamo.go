package awsDynamo

/*
   @author: vikram
*/
import (
	logger "github.com/astaxie/beego"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/controller/reqresp"
	apiError "payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/error"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/status"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/util/mathutil"
	"payu-sicarios.githost.io/payuhub/api.payuhub.com/settlement/data-store-service/util/stringutil"
	"sync"
	"time"
)

// TODO Code inside all functions might need to modify, Check with official documentation

const (
	equal               = "="
	notEqual            = "<>"
	lessThan            = "<"
	greaterThan         = ">"
	lessThanAndEqual    = "<="
	greaterThanAndEqual = ">="
	between             = "BETWEEN"
	in                  = "IN"
)

const (
	logicalOR  = "OR"
	logicalAND = "AND"
	logicalNOT = "NOT"
)

const (
	batchSize      = 4
	noOfGoroutines = 2
)

// OperationType
const (
	BatchDelete = "BatchDeleteItems"
	BatchInsert = "BatchInsertItems"
)

// Interface declare DAO layer function to interact with dynamoDB database
type Interface interface {
	InsertItem(tableName string, item map[string]interface{}) *reqresp.Response
	UpdateItem(tableName string, item map[string]interface{}, key map[string]interface{},
		conditions []reqresp.Condition) *reqresp.Response
	DeleteItem(tableName string, key map[string]interface{}, conditions []reqresp.Condition) *reqresp.Response
	BatchOperations(tableName string, items []map[string]interface{}, operationType string) *reqresp.Response
}

// Client struct have instance of dynamodb.DynamoDB and implements Interface
type Client struct {
	Client *dynamodb.DynamoDB
}

// InsertItem create or replace an item
// It takes tableName and itemData
// Return an error if occurred else nil
func (dynamoDB *Client) InsertItem(tableName string, item map[string]interface{}) (errorResponse *reqresp.Response) {

	var errCode int
	var errType string
	var errInfo string

	dynamoItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		errCode = status.UnProcessableEntity
		errType = apiError.APIRequestError
		errInfo = "Item cannot be marshaled into required DynamoDB format. " + err.Error()
		// return from here if error occurred
	}

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      dynamoItem,
	}

	_, err = dynamoDB.Client.PutItem(putItemInput)
	if err == nil {
		return nil
	}

	if awserr, ok := err.(awserr.Error); ok {
		switch awserr.Code() {
		case dynamodb.ErrCodeResourceNotFoundException:
			errCode = status.UnProcessableEntity
			errType = apiError.APIRequestError
			errInfo = "ResourceNotFoundException: " + awserr.Message() + ". Table: " + tableName + " may not exist"
		case "ValidationException":
			errCode = status.UnProcessableEntity
			errType = apiError.APIRequestError
			errInfo = "ValidationException: " + awserr.Message()
		case dynamodb.ErrCodeProvisionedThroughputExceededException:
			errCode = status.TooManyRequests
			errType = apiError.APIRequestError
			errInfo = "ProvisionedThroughputExceededException: The request rate is too high"
		case dynamodb.ErrCodeInternalServerError:
			errCode = status.ServiceUnavailable
			errType = apiError.APIServiceError
			errInfo = awserr.Message()
		default:
			errCode = status.ServiceUnavailable
			errType = apiError.APIServiceError
			errInfo = awserr.Message()
		}
	} else {
		errCode = status.InternalServerError
		errType = apiError.APIProcessingError
		errInfo = err.Error()
	}

	logger.Error("[InsertItem] " + errInfo)
	return &reqresp.Response{
		HTTPStatusCode: errCode,
		APIResponse:    apiError.New(errType, status.Desc(errCode), errInfo),
	}
}

// UpdateItem updates an item
// Update attributes is exist, add new attributes
// Update only when item already present in table
// It takes tableName, item, key and conditions
// Returns *reqresp.Response containing either success response or error response
func (dynamoDB *Client) UpdateItem(tableName string, item map[string]interface{}, key map[string]interface{},
	conditions []reqresp.Condition) *reqresp.Response {

	primaryKeys, mapMarshalErr := dynamodbattribute.MarshalMap(key)
	if mapMarshalErr != nil {
		return &reqresp.Response{
			APIResponse:    apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity), mapMarshalErr.Error()),
			HTTPStatusCode: status.UnProcessableEntity,
		}
	}
	updateItemInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key:       primaryKeys,

		ReturnValues: aws.String("ALL_NEW"),
	}

	expressionAttributeNames := map[string]*string{}
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{}

	var updateString = updateStatement(item, expressionAttributeNames, expressionAttributeValues)
	if stringutil.IsNotEmpty(updateString) {
		updateItemInput.UpdateExpression = aws.String(updateString)
	}

	var conditionString = conditionStatement(primaryKeys, conditions, expressionAttributeNames, expressionAttributeValues)
	if stringutil.IsNotEmpty(conditionString) {
		updateItemInput.ConditionExpression = aws.String(conditionString)
	}

	updateItemInput.ExpressionAttributeNames = expressionAttributeNames
	updateItemInput.ExpressionAttributeValues = expressionAttributeValues

	var errType, errInfo string
	var errCode int

	updateItemOutput, updateItemErr := dynamoDB.Client.UpdateItem(updateItemInput)
	if updateItemErr != nil {
		if awsErr, ok := updateItemErr.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				errType = apiError.APIRequestError
				errCode = status.UnProcessableEntity
				errInfo = "no such item found, error: " + awsErr.Message()
			case dynamodb.ErrCodeResourceNotFoundException:
				errType = apiError.APIRequestError
				errCode = status.UnProcessableEntity
				errInfo = "table: " + tableName + " doesn't exist"
			case dynamodb.ErrCodeInternalServerError:
				errType = apiError.APIServiceError
				errCode = status.ServiceUnavailable
				errInfo = awsErr.Message()
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				errType = apiError.APIRequestError
				errCode = status.TooManyRequests
				errInfo = "ProvisionedThroughputExceededException: The request rate is too high"
				// If no item in table satisfies the condition provided, this error occurred
			case "ValidationException":
				errType = apiError.APIRequestError
				errCode = status.UnProcessableEntity
				errInfo = "ValidationException: " + awsErr.Message()

			default:
				errType = apiError.APIProcessingError
				errCode = status.InternalServerError
				errInfo = awsErr.Message()

			}
			logger.Error("[UpdateItem]." + errInfo)
			return &reqresp.Response{
				APIResponse:    apiError.New(errType, status.Desc(errCode), errInfo),
				HTTPStatusCode: errCode,
			}
		}
		logger.Error("[UpdateItem]." + updateItemErr.Error())

		return &reqresp.Response{
			APIResponse:    apiError.New(apiError.APIProcessingError, status.Desc(status.InternalServerError), updateItemErr.Error()),
			HTTPStatusCode: status.InternalServerError,
		}
	}

	logger.Info("[UpdateItem].Successfully update the item in table: "+tableName+", with primaryKeys: ", primaryKeys)
	var updatedItem map[string]interface{}
	unmarshalMapErr := dynamodbattribute.UnmarshalMap(updateItemOutput.Attributes, &updatedItem)
	if unmarshalMapErr != nil {
		logger.Error("[UpdateItem].Error occurred while Unmarshal the updatedItem, Error: ", unmarshalMapErr)
	}
	return &reqresp.Response{
		APIResponse:    updatedItem,
		HTTPStatusCode: status.OK,
	}
}

// DeleteItem deletes an item from table and have idempotent response
// It takes tableName, key and conditions.
// Delete if item exists and satisfies the conditions
// Return an error if occurred else nil.
func (dynamoDB *Client) DeleteItem(tableName string, key map[string]interface{}, conditions []reqresp.Condition) *reqresp.Response {

	primaryKeys, mapMarshalErr := dynamodbattribute.MarshalMap(key)
	if mapMarshalErr != nil {
		return &reqresp.Response{
			APIResponse:    apiError.New(apiError.APIRequestError, status.Desc(status.UnProcessableEntity), mapMarshalErr.Error()),
			HTTPStatusCode: status.UnProcessableEntity,
		}
	}

	deleteItemInput := &dynamodb.DeleteItemInput{
		TableName:    aws.String(tableName),
		Key:          primaryKeys,
		ReturnValues: aws.String("ALL_OLD"),
	}

	expressionAttributeNames := map[string]*string{}
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{}

	var conditionString = conditionStatement(primaryKeys, conditions, expressionAttributeNames, expressionAttributeValues)
	if stringutil.IsNotEmpty(conditionString) {
		deleteItemInput.ConditionExpression = aws.String(conditionString)
	}

	deleteItemInput.ExpressionAttributeNames = expressionAttributeNames
	deleteItemInput.ExpressionAttributeValues = expressionAttributeValues

	var errType, errInfo string
	var errCode int

	_, deleteItemErr := dynamoDB.Client.DeleteItem(deleteItemInput)
	if deleteItemErr != nil {
		if awsErr, ok := deleteItemErr.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				logger.Info("[DeleteItem].No such item found in table:", tableName, "with keys:", key, "and satisfying condition:", conditions)
				return &reqresp.Response{
					HTTPStatusCode: status.NoContent,
				}
			case dynamodb.ErrCodeResourceNotFoundException:
				errType = apiError.APIRequestError
				errCode = status.UnProcessableEntity
				errInfo = "table: " + tableName + " doesn't exist"
			case dynamodb.ErrCodeInternalServerError:
				errType = apiError.APIServiceError
				errCode = status.ServiceUnavailable
				errInfo = awsErr.Message()
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				errType = apiError.APIRequestError
				errCode = status.TooManyRequests
				errInfo = "ProvisionedThroughputExceededException: The request rate is too high"
			case "ValidationException":
				errType = apiError.APIRequestError
				errCode = status.UnProcessableEntity
				errInfo = "ValidationException: " + awsErr.Message()

			default:
				errType = apiError.APIProcessingError
				errCode = status.InternalServerError
				errInfo = awsErr.Message()

			}
			logger.Error("[DeleteItem]." + errInfo)
			return &reqresp.Response{
				APIResponse:    apiError.New(errType, status.Desc(errCode), errInfo),
				HTTPStatusCode: errCode,
			}

		}
		logger.Error("[DeleteItem]." + deleteItemErr.Error())
		return &reqresp.Response{
			APIResponse:    apiError.New(apiError.APIProcessingError, status.Desc(status.InternalServerError), deleteItemErr.Error()),
			HTTPStatusCode: status.InternalServerError,
		}
	}

	logger.Info("[DeleteItem].Successfully deleted the item from table:", tableName, "with keys: ", key)
	return &reqresp.Response{
		HTTPStatusCode: status.NoContent,
	}
}

//BatchOperations is the service for the controller function BatchOperations.
//It calls the dynamodb Batch write item service and then if not any error occurred return the items collection
func (dynamoDB *Client) BatchOperations(tableName string, items []map[string]interface{}, operationType string) *reqresp.Response {

	var wg sync.WaitGroup
	logger.Info("[BatchOperations].Processing Request, items size:", len(items))
	var data []map[string]*dynamodb.AttributeValue
	for _, item := range items {
		item, marshalMapErr := dynamodbattribute.MarshalMap(item)
		if marshalMapErr != nil {

		}
		data = append(data, item)
	}
	channelLoadData := make(chan []map[string]*dynamodb.AttributeValue)

	wg.Add(noOfGoroutines)
	for i := 0; i < noOfGoroutines; i++ {
		go dynamoDB.uploadDataToDB(channelLoadData, tableName, operationType, &wg)
	}

	for i := 0; i < len(data); i += batchSize {
		channelLoadData <- data[i:mathutil.Min(i+batchSize, len(data))]
	}

	close(channelLoadData)
	wg.Wait()
	logger.Info("[BatchOperations].OK writing Batch data to dynamoDB")

	return &reqresp.Response{
		APIResponse:    map[string]string{"count": "0"},
		HTTPStatusCode: status.OK,
	}

}

func (dynamoDB *Client) uploadDataToDB(channelLoad <-chan []map[string]*dynamodb.AttributeValue, tableName string,
	operationType string, wg *sync.WaitGroup) {

	logger.Info("[BatchOperations].Writing Batch data to dynamoDB")
	var items []map[string]*dynamodb.AttributeValue
	for items = range channelLoad {
		dynamoDB.batchOperation(tableName, items, operationType)
	}
	logger.Info("[BatchOperations].Finish writing Batch data to dynamoDB")
	wg.Done()
}

// batchOperation writes items in batch.
// It takes tableName and  []map[string]*dynamodb.AttributeValue.
func (dynamoDB *Client) batchOperation(tableName string, items []map[string]*dynamodb.AttributeValue, operationType string) {

	var requestSlice []*dynamodb.WriteRequest
	requestItems := make(map[string][]*dynamodb.WriteRequest)

	switch operationType {
	case BatchInsert:
		for _, item := range items {
			var putRequest = &dynamodb.PutRequest{Item: item}
			var writeRequest = dynamodb.WriteRequest{PutRequest: putRequest}
			requestSlice = append(requestSlice, &writeRequest)

		}
	case BatchDelete:
		for _, item := range items {
			var deleteRequest = &dynamodb.DeleteRequest{Key: item}
			var writeRequest = dynamodb.WriteRequest{DeleteRequest: deleteRequest}
			requestSlice = append(requestSlice, &writeRequest)
		}
	}

	requestItems[tableName] = requestSlice
	batchWriteItemInput := dynamodb.BatchWriteItemInput{RequestItems: requestItems}
	batchWriteItemOutput, err := dynamoDB.Client.BatchWriteItem(&batchWriteItemInput)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				logger.Error("[BatchOperations].Table: " + tableName + " doesn't exist")
			case dynamodb.ErrCodeInternalServerError:
				logger.Error("[BatchOperations]." + awsErr.Message())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				logger.Error("[BatchOperations].Your request rate is too high, ProvisionedThroughputExceededException")
			case dynamodb.ErrCodeConditionalCheckFailedException:
				logger.Error("[BatchOperations].The conditional request failed, ConditionalCheckFailedException")
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				logger.Error("[UpdateItem].An item collection is too large, ItemCollectionSizeLimitExceededException")
			case "ValidationException":
				logger.Error("[BatchOperations].Missing one of the primaryKey, invalid request") // %v", primaryKey)
			default:
				logger.Error("[BatchOperations].Error: " + awsErr.Message())
			}
		}
	}

	for {
		if len(batchWriteItemOutput.UnprocessedItems) > 0 {
			//TODO if failed to process every time
			time.Sleep(5 * time.Millisecond)
			batchWriteItemInput.RequestItems = batchWriteItemOutput.UnprocessedItems
			batchWriteItemOutput, err = dynamoDB.Client.BatchWriteItem(&batchWriteItemInput)
		} else {
			break
		}
	}
}

// updateStatement to generate updateString
func updateStatement(item map[string]interface{}, expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue) string {

	var updateString = "SET"
	for key, value := range item {
		expressionAttributeNames["#u"+key] = aws.String(key)
		attributeValue, marshalErr := dynamodbattribute.Marshal(value)
		if marshalErr != nil {
			logger.Error("[updateStatement].Error:", marshalErr)
		}
		expressionAttributeValues[":u"+key] = attributeValue
		updateString = updateString + " #u" + key + " = :u" + key + ","
	}
	updateString = updateString[:len(updateString)-len(",")]
	return updateString
}

// conditionStatement to generate conditionString
func conditionStatement(keys map[string]*dynamodb.AttributeValue, conditions []reqresp.Condition, expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue) string {
	var conditionString string

	for key, value := range keys {
		expressionAttributeNames["#c"+key] = aws.String(key)
		expressionAttributeValues[":c"+key] = value
		conditionString = conditionString + " #c" + key + " " + equal + " :c" + key + " " + logicalAND
	}
	if len(conditions) == 0 {
		conditionString = conditionString[:len(conditionString)-len(logicalAND)]
	}

	for _, condition := range conditions {
		expressionAttributeNames["#c"+condition.Key] = aws.String(condition.Key)
		attributeValue, marshalErr := dynamodbattribute.Marshal(condition.Value)
		if marshalErr != nil {
			logger.Error("[conditionStatement].Error:", marshalErr)
		}
		expressionAttributeValues[":c"+condition.Key] = attributeValue

		conditionString = conditionString + " #c" + condition.Key + " " + condition.Operation + " :c" + condition.Key + " " + condition.Relation
	}
	return conditionString
}
