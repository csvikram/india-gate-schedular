package reqresp

/*
   @author: vikram
*/

// Text contains string field.
type Text struct {
	Text string `json:"text,omitempty"`
}

// S3Request used for parsing s3 object store api
// It have three fields, Bucket, ObjectName and Data
type S3Request struct {
	Bucket     string `json:"bucket"`
	ObjectName string `json:"objectName"`
	Data       []byte `json:"data"`
}

// DBRequest is the request body for all DB APIs
type DBRequest struct {
	TableName           string                   `json:"tableName"`
	Item                map[string]interface{}   `json:"item,omitempty"`
	Key                 map[string]interface{}   `json:"key,omitempty"`
	ConditionExpression []Condition              `json:"conditionExpression,omitempty"`
	Items               []map[string]interface{} `json:"items,omitempty"`
	Keys                []map[string]interface{} `json:"keys,omitempty"`
}

// Condition contains fields for specifying conditions for db operations
type Condition struct {
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Operation string      `json:"operation"`
	Relation  string      `json:"relation"`
}
