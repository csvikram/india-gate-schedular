package main

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"os"
)

const TableNameInsertData = "TABLE_NAME"

func insertEventInDB(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	tableName := os.Getenv(TableNameInsertData)
	region := "us-east-1"
	//session
	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	//database client
	dbClient := dynamodb.New(awsSession)

	var body map[string]interface{}

	if err := json.Unmarshal([]byte(request.Body), &body); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       string("Something went wrong"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
		}, nil
	}

	dynamoItem, err := dynamodbattribute.MarshalMap(&body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Something went wrong" + err.Error(),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
		}, nil

	}
	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      dynamoItem,
	}

	_, err = dbClient.PutItem(putItemInput)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Something went wrong" + err.Error(),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       "SuccessFully insert data",
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
	}, nil

}

func main() {
	lambda.Start(insertEventInDB)
}
