package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"os"
	"time"
	"log"
)

const TableNameInsertDataCloudWatch = "TABLE_NAME"

func insertEventInDBCloudWatch(request string)  error{

	log.Println("Running inserEventINDB" + request)
	region := "us-east-1"
	tableName := os.Getenv(TableNameInsertDataCloudWatch)
	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	//database client
	dbClient := dynamodb.New(awsSession)

	var body map[string]interface{}

	body["eventID"] = time.Now().String()

	dynamoItem, _ := dynamodbattribute.MarshalMap(&body)

	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      dynamoItem,
	}

	_, err := dbClient.PutItem(putItemInput)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("Done")
	return nil
}

func main() {
	lambda.Start(insertEventInDBCloudWatch)
}
