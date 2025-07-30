package ctprocessor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client interface defines the methods for S3 operations
type S3Client interface {
	ListObjectsV2(ctx context.Context, bucket, prefix string, continuationToken string) (*ListObjectsV2Output, error)
	DownloadFile(ctx context.Context, bucket, key, localPath string) error
	GetObjectAttributes(ctx context.Context, bucket, key string) (*ObjectAttributes, error)
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
}

// ListObjectsV2Output represents the output of ListObjectsV2 operation
type ListObjectsV2Output struct {
	Objects               []S3Object
	NextContinuationToken string
	IsTruncated          bool
}

// ObjectAttributes represents S3 object attributes
type ObjectAttributes struct {
	ETag string
	Size int64
}

// AWSS3Client is the concrete implementation of S3Client using AWS SDK
type AWSS3Client struct {
	client *s3.Client
	config aws.Config
	bucket string
}

// NewAWSS3Client creates a new AWS S3 client with credential chain support
func NewAWSS3Client(ctx context.Context, region, bucket string) (*AWSS3Client, error) {
	// Load AWS configuration with credential chain support
	// This supports environment variables, IAM roles, and profiles
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(3),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &AWSS3Client{
		client: client,
		config: cfg,
		bucket: bucket,
	}, nil
}

// NewAWSS3ClientWithConfig creates a new AWS S3 client with custom configuration
func NewAWSS3ClientWithConfig(cfg aws.Config, bucket string) *AWSS3Client {
	client := s3.NewFromConfig(cfg)
	return &AWSS3Client{
		client: client,
		config: cfg,
		bucket: bucket,
	}
}

// ListObjectsV2 lists objects in an S3 bucket with pagination support
func (c *AWSS3Client) ListObjectsV2(ctx context.Context, bucket, prefix string, continuationToken string) (*ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}

	result, err := c.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in bucket %s: %w", bucket, err)
	}

	// Convert AWS SDK objects to our S3Object type
	objects := make([]S3Object, len(result.Contents))
	for i, obj := range result.Contents {
		objects[i] = S3Object{
			Key:          aws.ToString(obj.Key),
			Size:         aws.ToInt64(obj.Size),
			ETag:         aws.ToString(obj.ETag),
			LastModified: aws.ToTime(obj.LastModified),
		}
	}

	output := &ListObjectsV2Output{
		Objects:     objects,
		IsTruncated: aws.ToBool(result.IsTruncated),
	}

	if result.NextContinuationToken != nil {
		output.NextContinuationToken = aws.ToString(result.NextContinuationToken)
	}

	return output, nil
}

// DownloadFile downloads a file from S3 to a local path
func (c *AWSS3Client) DownloadFile(ctx context.Context, bucket, key, localPath string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Get object from S3
	result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object %s from bucket %s: %w", key, bucket, err)
	}
	defer result.Body.Close()

	// Create temporary file for atomic write
	tempPath := localPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file %s: %w", tempPath, err)
	}

	// Copy data from S3 to file
	_, err = io.Copy(file, result.Body)
	closeErr := file.Close()

	if err != nil {
		os.Remove(tempPath) // Clean up temp file on error
		return fmt.Errorf("failed to write data to file %s: %w", tempPath, err)
	}

	if closeErr != nil {
		os.Remove(tempPath) // Clean up temp file on error
		return fmt.Errorf("failed to close file %s: %w", tempPath, closeErr)
	}

	// Atomically move temp file to final location
	if err := os.Rename(tempPath, localPath); err != nil {
		os.Remove(tempPath) // Clean up temp file on error
		return fmt.Errorf("failed to move temporary file to final location %s: %w", localPath, err)
	}

	return nil
}

// GetObjectAttributes retrieves object attributes (ETag and Size) from S3
func (c *AWSS3Client) GetObjectAttributes(ctx context.Context, bucket, key string) (*ObjectAttributes, error) {
	result, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object attributes for %s in bucket %s: %w", key, bucket, err)
	}

	return &ObjectAttributes{
		ETag: aws.ToString(result.ETag),
		Size: aws.ToInt64(result.ContentLength),
	}, nil
}

// GetConfig returns the AWS configuration used by this client
func (c *AWSS3Client) GetConfig() aws.Config {
	return c.config
}

// GetObject gets an object from S3 and returns a ReadCloser
func (c *AWSS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s from bucket %s: %w", key, c.bucket, err)
	}

	return result.Body, nil
}

// GetRegion returns the region configured for this client
func (c *AWSS3Client) GetRegion() string {
	return c.config.Region
}