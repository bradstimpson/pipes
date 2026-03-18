package util

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWS SDK v2 S3 utils
func ListS3ObjectsV2(ctx context.Context, client *s3.Client, bucket, keyPrefix string) ([]string, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(keyPrefix),
	}
	objects := []string{}
	paginator := s3.NewListObjectsV2Paginator(client, params)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			objects = append(objects, *o.Key)
		}
	}
	return objects, nil
}

func GetS3ObjectV2(ctx context.Context, client *s3.Client, bucket, objKey string) (*s3.GetObjectOutput, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objKey),
	}
	return client.GetObject(ctx, params)
}

func DeleteS3ObjectsV2(ctx context.Context, client *s3.Client, bucket string, objKeys []string) (*s3.DeleteObjectsOutput, error) {
	s3Ids := make([]types.ObjectIdentifier, len(objKeys))
	for i, key := range objKeys {
		s3Ids[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}
	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: s3Ids,
			Quiet:   aws.Bool(true),
		},
	}
	return client.DeleteObjects(ctx, params)
}

func WriteS3ObjectV2(ctx context.Context, data []string, client *s3.Client, bucket string, key string, lineSeparator string, compress bool) (string, error) {
	// For simplicity, compression is omitted here. Add gzip logic if needed.
	body := strings.NewReader(strings.Join(data, lineSeparator))
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	_, err := client.PutObject(ctx, params)
	return key, err
}

// StubHTTPClient returns an http.Client that never actually connects.
func StubHTTPClient() aws.HTTPClient {
	return &http.Client{
		Transport: &roundTripperStub{},
	}
}

type roundTripperStub struct{}

func (r *roundTripperStub) RoundTrip(req *http.Request) (*http.Response, error) {
	// Return stubbed responses based on request
	switch {
	case req.Method == "GET" && strings.Contains(req.URL.RawQuery, "list-type=2"):
		// ListObjectsV2: return XML with two keys
		xml := `<?xml version="1.0" encoding="UTF-8"?>
<ListObjectsV2Response>
	<Contents>
		<Key>file1</Key>
	</Contents>
	<Contents>
		<Key>file2</Key>
	</Contents>
</ListObjectsV2Response>`
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(xml)),
			Header:     make(http.Header),
		}, nil
	case req.Method == "GET" && strings.Contains(req.URL.RawQuery, "x-id=GetObject"):
		// GetObject: return testdata
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("testdata")),
			Header:     make(http.Header),
		}, nil
	default:
		// Default: empty response
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	}
}
