package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"os"
	"github.com/aws/aws-lambda-go/lambda"
)

var TASK_QUEUE_URL = os.Getenv("TASK_QUEUE_URL")
var WORKER_LAMBDA_NAME = os.Getenv("WORKER_LAMBDA_NAME")
var AWS_REGION = os.Getenv("AWS_REGION")
var max_no_messages int64 = 10

func handleWormHoleMessages(context map[string]interface{}) {

	// receive messages from worm hole
	log.Print("Consuming from worm hole")
	region := "us-east-1"

	awsSession := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))

	sqsClient := sqs.New(awsSession)

	var messages = &sqs.ReceiveMessageOutput{}



	messages, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{QueueUrl: &TASK_QUEUE_URL, MaxNumberOfMessages: &max_no_messages})

	if err != nil {
		log.Fatal("Error while getting messages from Worm Hole")
	}

	if len(messages.Messages) > 0 {
		log.Print("Received something")
	}
	// call worker lambda

}

func main() {

	lambda.Start(handleWormHoleMessages)
}
