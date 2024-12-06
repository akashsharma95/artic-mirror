package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	client *s3.Client
	bucket string
	prefix string
	buffer *Buffer
}

func NewS3Storage(client *s3.Client, bucket, prefix string) *S3Storage {
	return &S3Storage{
		client: client,
		bucket: bucket,
		prefix: prefix,
		buffer: NewBuffer(),
	}
}

func (s *S3Storage) Write(ctx context.Context, filepath string, data io.Reader) error {
	fullPath := path.Join(s.prefix, filepath)

	// Write data to buffer
	if _, err := io.Copy(s.buffer, data); err != nil {
		return fmt.Errorf("copying data to buffer: %w", err)
	}

	// Read data from buffer
	bufferData := s.buffer.Reader()

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
		Body:   bufferData,
	})
	if err != nil {
		return fmt.Errorf("putting object: %w", err)
	}

	// Reset buffer after writing to S3
	s.buffer.Reset()

	return nil
}

func (s *S3Storage) Read(ctx context.Context, filepath string) (io.ReadCloser, error) {
	fullPath := path.Join(s.prefix, filepath)

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
	})
	if err != nil {
		return nil, fmt.Errorf("getting object: %w", err)
	}

	return output.Body, nil
}

func (s *S3Storage) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := path.Join(s.prefix, prefix)
	var files []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects: %w", err)
		}

		for _, obj := range page.Contents {
			files = append(files, strings.TrimPrefix(*obj.Key, s.prefix+"/"))
		}
	}

	return files, nil
}
