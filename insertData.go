package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/csvikram/india-gate-schedular/services/awsDynamo"
	"encoding/json"
)

func insertEventInDB(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	region := "us-east-1"
	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	dbClient := &awsDynamo.Client{Client: dynamodb.New(awsSession)}
	var body map[string]interface{}

	if err := json.Unmarshal([]byte(request.Body),&body); err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       string("Something went wrong"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
		}, nil
	}
	dbClient.InsertItem("test_lambda",body)

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string("this is main, test 3"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
	}, nil

}

func main() {
	lambda.Start(insertEventInDB)
}
