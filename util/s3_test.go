package util

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

func stubS3ClientListObjectsV2(keys []string) *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       StubHTTPClient(),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("StubListObjectsV2", func(ctx context.Context, input middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
					contents := make([]types.Object, len(keys))
					for i := range keys {
						contents[i] = types.Object{Key: &keys[i]}
					}
					return middleware.SerializeOutput{Result: &s3.ListObjectsV2Output{Contents: contents}}, middleware.Metadata{}, nil
				}), "", middleware.Before)
				return nil
			},
		},
	})
}

func stubS3ClientGetObject(data string) *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       StubHTTPClient(),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("StubGetObject", func(ctx context.Context, input middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
					body := io.NopCloser(strings.NewReader(data))
					return middleware.SerializeOutput{Result: &s3.GetObjectOutput{Body: body}}, middleware.Metadata{}, nil
				}), "", middleware.Before)
				return nil
			},
		},
	})
}

func stubS3ClientPutObject() *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       StubHTTPClient(),
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

func stubS3ClientDeleteObjects() *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		EndpointResolver: s3.EndpointResolverFromURL("http://localhost"),
		HTTPClient:       StubHTTPClient(),
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("StubDeleteObjects", func(ctx context.Context, input middleware.SerializeInput, next middleware.SerializeHandler) (middleware.SerializeOutput, middleware.Metadata, error) {
					return middleware.SerializeOutput{Result: &s3.DeleteObjectsOutput{}}, middleware.Metadata{}, nil
				}), "", middleware.Before)
				return nil
			},
		},
	})
}

func TestListS3ObjectsV2(t *testing.T) {
	client := stubS3ClientListObjectsV2([]string{"file1", "file2"})
	keys, err := ListS3ObjectsV2(context.TODO(), client, "bucket", "prefix")
	if err != nil {
		t.Fatalf("ListS3ObjectsV2 failed: %v", err)
	}
	if len(keys) != 2 || keys[0] != "file1" || keys[1] != "file2" {
		t.Errorf("Unexpected keys: %v", keys)
	}
}

func TestGetS3ObjectV2(t *testing.T) {
	client := stubS3ClientGetObject("testdata")
	obj, err := GetS3ObjectV2(context.TODO(), client, "bucket", "key")
	if err != nil {
		t.Fatalf("GetS3ObjectV2 failed: %v", err)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, obj.Body)
	if err != nil {
		t.Fatalf("Failed to read object body: %v", err)
	}
	if buf.String() != "testdata" {
		t.Errorf("Unexpected object body: %s", buf.String())
	}
}

func TestWriteS3ObjectV2(t *testing.T) {
	client := stubS3ClientPutObject()
	key, err := WriteS3ObjectV2(context.TODO(), []string{"a", "b"}, client, "bucket", "key", "\n", false)
	if err != nil {
		t.Fatalf("WriteS3ObjectV2 failed: %v", err)
	}
	if key != "key" {
		t.Errorf("Unexpected key: %s", key)
	}
}

func TestDeleteS3ObjectsV2(t *testing.T) {
	client := stubS3ClientDeleteObjects()
	_, err := DeleteS3ObjectsV2(context.TODO(), client, "bucket", []string{"key1", "key2"})
	if err != nil {
		t.Fatalf("DeleteS3ObjectsV2 failed: %v", err)
	}
}
