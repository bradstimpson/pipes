package processors

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/util"
)

// S3Writer sends data upstream to S3. By default, we will not compress data before sending it.
// Set the `Compress` flag to true to use gzip compression before storing in S3 (if this flag is
// set to true, ".gz" will automatically be appended to the key name specified).
//
// By default, we will separate each iteration of data sent to `ProcessData` with a new line
// when we piece back together to send to S3. Change the `LineSeparator` attribute to change
// this behavior.
type S3Writer struct {
	data          []string
	Compress      bool
	LineSeparator string
	client        *s3.Client
	bucket        string
	key           string
}

// NewS3Writer instaniates a new S3Writer
func NewS3Writer(awsID, awsSecret, awsRegion, bucket, key string) *S3Writer {
	w := S3Writer{bucket: bucket, key: key, LineSeparator: "\n", Compress: false}
	// Use AWS SDK v2 config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsID, awsSecret, "",
		)),
	)
	if err != nil {
		panic("failed to load AWS config: " + err.Error())
	}
	w.client = s3.NewFromConfig(cfg)
	return &w
}

// SetClient overrides the S3 client (useful for testing/benchmarks).
func (w *S3Writer) SetClient(c *s3.Client) {
	w.client = c
}

// ProcessData enqueues all received data
func (w *S3Writer) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	w.data = append(w.data, string(d))
}

// Finish writes all enqueued data to S3, defering to util.WriteS3Object
func (w *S3Writer) Finish(outputChan chan data.JSON, killChan chan error) {
	util.WriteS3ObjectV2(context.TODO(), w.data, w.client, w.bucket, w.key, w.LineSeparator, w.Compress)
}

func (w *S3Writer) String() string {
	return "S3Writer"
}
