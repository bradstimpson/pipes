package processors

import (
	"context"
	"testing"

	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/util"
)

func stubS3GetObject() *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       StubHTTPClientForTest(),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("StubGetObject", func(ctx context.Context, input middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
					body := io.NopCloser(strings.NewReader("testdata"))
					return middleware.SerializeOutput{Result: &s3.GetObjectOutput{Body: body}}, middleware.Metadata{}, nil
				}), "", middleware.Before)
				return nil
			},
		},
	})
}

// Provide a wrapper for util.StubHTTPClient for test import clarity
func StubHTTPClientForTest() aws.HTTPClient {
	return util.StubHTTPClient()
}
func TestS3Reader_ProcessData_Stub(t *testing.T) {
	r := &S3Reader{
		IoReader: IoReader{LineByLine: true},
		client:   stubS3GetObject(),
		bucket:   "test-bucket",
		object:   "test-object",
	}
	outputChan := make(chan data.JSON, 1)
	killChan := make(chan error, 1)
	// Debug: print before calling processObject
	r.processObject(
		&s3.GetObjectOutput{
			Body: io.NopCloser(strings.NewReader("testdata")),
		}, outputChan, killChan,
	)
	// Debug: print after calling processObject
	select {
	case err := <-killChan:
		t.Fatalf("ProcessData error: %v", err)
	case out := <-outputChan:
		if string(out) != "testdata" {
			t.Fatalf("Unexpected output: %s", out)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("No output or error received (timeout)")
	}
}

// smithy stub client
func stubS3PutObject() *s3.Client {
	return s3.New(s3.Options{
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("StubPutObject", func(ctx context.Context, input middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
					return middleware.SerializeOutput{Result: &s3.PutObjectOutput{}}, middleware.Metadata{}, nil
				}), "", middleware.Before)
				return nil
			},
		},
	})
}

func TestS3Writer_ProcessData_Stub(t *testing.T) {
	writer := &S3Writer{
		client:        stubS3PutObject(),
		bucket:        "test-bucket",
		key:           "test-key",
		LineSeparator: "\n",
		Compress:      false,
	}
	outputChan := make(chan data.JSON, 1)
	killChan := make(chan error, 1)
	writer.ProcessData([]byte("testdata"), outputChan, killChan)
	writer.Finish(outputChan, killChan)
}
