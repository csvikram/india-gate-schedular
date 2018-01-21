package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func helloHandler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	//index, err := ioutil.ReadFile("public/index.html")
	//if err != nil {
	//	return events.APIGatewayProxyResponse{}, err
	//}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string("hello world, Test 3"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
	}, nil

}

func main() {
	lambda.Start(helloHandler)
}
