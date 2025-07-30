package ctprocessor

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MockS3API implements the S3 API interface for testing
type MockS3API struct {
	ListObjectsV2Func func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObjectFunc     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObjectFunc    func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

func (m *MockS3API) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.ListObjectsV2Func != nil {
		return m.ListObjectsV2Func(ctx, params, optFns...)
	}
	return nil, errors.New("ListObjectsV2Func not implemented")
}

func (m *MockS3API) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFunc != nil {
		return m.GetObjectFunc(ctx, params, optFns...)
	}
	return nil, errors.New("GetObjectFunc not implemented")
}

func (m *MockS3API) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.HeadObjectFunc != nil {
		return m.HeadObjectFunc(ctx, params, optFns...)
	}
	return nil, errors.New("HeadObjectFunc not implemented")
}

// MockS3Client wraps the mock API for testing
type MockS3Client struct {
	mockAPI *MockS3API
	config  aws.Config
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		mockAPI: &MockS3API{},
		config:  aws.Config{Region: "us-east-1"},
	}
}

func (c *MockS3Client) ListObjectsV2(ctx context.Context, bucket, prefix string, continuationToken string) (*ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}

	result, err := c.mockAPI.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err
	}

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

func (c *MockS3Client) DownloadFile(ctx context.Context, bucket, key, localPath string) error {
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	result, err := c.mockAPI.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer result.Body.Close()

	tempPath := localPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, result.Body)
	closeErr := file.Close()

	if err != nil {
		os.Remove(tempPath)
		return err
	}

	if closeErr != nil {
		os.Remove(tempPath)
		return closeErr
	}

	return os.Rename(tempPath, localPath)
}

func (c *MockS3Client) GetObjectAttributes(ctx context.Context, bucket, key string) (*ObjectAttributes, error) {
	result, err := c.mockAPI.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return &ObjectAttributes{
		ETag: aws.ToString(result.ETag),
		Size: aws.ToInt64(result.ContentLength),
	}, nil
}

func (c *MockS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	// For testing, we'll use a default bucket name
	result, err := c.mockAPI.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

// Test helper to create a mock body reader
type mockBody struct {
	content string
	reader  *strings.Reader
}

func newMockBody(content string) *mockBody {
	return &mockBody{
		content: content,
		reader:  strings.NewReader(content),
	}
}

func (m *mockBody) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *mockBody) Close() error {
	return nil
}

func TestNewAWSS3Client(t *testing.T) {
	ctx := context.Background()
	
	// This test will fail in environments without AWS credentials
	// but demonstrates the interface
	_, err := NewAWSS3Client(ctx, "us-east-1", "test-bucket")
	
	// We expect this to either succeed or fail with a credential error
	// The important thing is that the function doesn't panic
	if err != nil {
		t.Logf("Expected credential error in test environment: %v", err)
	}
}

func TestMockS3Client_ListObjectsV2(t *testing.T) {
	tests := []struct {
		name              string
		bucket            string
		prefix            string
		continuationToken string
		mockResponse      *s3.ListObjectsV2Output
		mockError         error
		expectedObjects   int
		expectError       bool
	}{
		{
			name:   "successful list with objects",
			bucket: "test-bucket",
			prefix: "logs/",
			mockResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{
						Key:          aws.String("logs/file1.gz"),
						Size:         aws.Int64(1024),
						ETag:         aws.String("\"abc123\""),
						LastModified: aws.Time(time.Now()),
					},
					{
						Key:          aws.String("logs/file2.gz"),
						Size:         aws.Int64(2048),
						ETag:         aws.String("\"def456\""),
						LastModified: aws.Time(time.Now()),
					},
				},
				IsTruncated: aws.Bool(false),
			},
			expectedObjects: 2,
			expectError:     false,
		},
		{
			name:   "empty bucket",
			bucket: "empty-bucket",
			mockResponse: &s3.ListObjectsV2Output{
				Contents:    []types.Object{},
				IsTruncated: aws.Bool(false),
			},
			expectedObjects: 0,
			expectError:     false,
		},
		{
			name:        "API error",
			bucket:      "error-bucket",
			mockError:   errors.New("access denied"),
			expectError: true,
		},
		{
			name:              "pagination with continuation token",
			bucket:            "paginated-bucket",
			continuationToken: "token123",
			mockResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{
						Key:          aws.String("page2/file1.gz"),
						Size:         aws.Int64(512),
						ETag:         aws.String("\"ghi789\""),
						LastModified: aws.Time(time.Now()),
					},
				},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("token456"),
			},
			expectedObjects: 1,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewMockS3Client()
			client.mockAPI.ListObjectsV2Func = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				// Verify input parameters
				if aws.ToString(params.Bucket) != tt.bucket {
					t.Errorf("Expected bucket %s, got %s", tt.bucket, aws.ToString(params.Bucket))
				}
				
				if tt.prefix != "" && aws.ToString(params.Prefix) != tt.prefix {
					t.Errorf("Expected prefix %s, got %s", tt.prefix, aws.ToString(params.Prefix))
				}
				
				if tt.continuationToken != "" && aws.ToString(params.ContinuationToken) != tt.continuationToken {
					t.Errorf("Expected continuation token %s, got %s", tt.continuationToken, aws.ToString(params.ContinuationToken))
				}

				if tt.mockError != nil {
					return nil, tt.mockError
				}
				return tt.mockResponse, nil
			}

			ctx := context.Background()
			result, err := client.ListObjectsV2(ctx, tt.bucket, tt.prefix, tt.continuationToken)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result.Objects) != tt.expectedObjects {
				t.Errorf("Expected %d objects, got %d", tt.expectedObjects, len(result.Objects))
			}

			// Verify pagination fields
			if tt.mockResponse.IsTruncated != nil {
				if result.IsTruncated != aws.ToBool(tt.mockResponse.IsTruncated) {
					t.Errorf("Expected IsTruncated %v, got %v", aws.ToBool(tt.mockResponse.IsTruncated), result.IsTruncated)
				}
			}

			if tt.mockResponse.NextContinuationToken != nil {
				if result.NextContinuationToken != aws.ToString(tt.mockResponse.NextContinuationToken) {
					t.Errorf("Expected NextContinuationToken %s, got %s", aws.ToString(tt.mockResponse.NextContinuationToken), result.NextContinuationToken)
				}
			}
		})
	}
}

func TestMockS3Client_DownloadFile(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		key          string
		localPath    string
		mockContent  string
		mockError    error
		expectError  bool
		expectFile   bool
	}{
		{
			name:        "successful download",
			bucket:      "test-bucket",
			key:         "test-file.gz",
			localPath:   "test-output/downloaded-file.gz",
			mockContent: "test file content",
			expectError: false,
			expectFile:  true,
		},
		{
			name:        "download with nested directory",
			bucket:      "test-bucket",
			key:         "nested/path/file.gz",
			localPath:   "test-output/nested/downloaded-file.gz",
			mockContent: "nested file content",
			expectError: false,
			expectFile:  true,
		},
		{
			name:        "API error",
			bucket:      "error-bucket",
			key:         "error-file.gz",
			localPath:   "test-output/error-file.gz",
			mockError:   errors.New("object not found"),
			expectError: true,
			expectFile:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before test
			os.RemoveAll("test-output")
			defer os.RemoveAll("test-output")

			client := NewMockS3Client()
			client.mockAPI.GetObjectFunc = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				// Verify input parameters
				if aws.ToString(params.Bucket) != tt.bucket {
					t.Errorf("Expected bucket %s, got %s", tt.bucket, aws.ToString(params.Bucket))
				}
				
				if aws.ToString(params.Key) != tt.key {
					t.Errorf("Expected key %s, got %s", tt.key, aws.ToString(params.Key))
				}

				if tt.mockError != nil {
					return nil, tt.mockError
				}

				return &s3.GetObjectOutput{
					Body: newMockBody(tt.mockContent),
				}, nil
			}

			ctx := context.Background()
			err := client.DownloadFile(ctx, tt.bucket, tt.key, tt.localPath)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectFile {
				// Verify file was created
				if _, err := os.Stat(tt.localPath); os.IsNotExist(err) {
					t.Error("Expected file to be created but it doesn't exist")
					return
				}

				// Verify file content
				content, err := os.ReadFile(tt.localPath)
				if err != nil {
					t.Errorf("Failed to read downloaded file: %v", err)
					return
				}

				if string(content) != tt.mockContent {
					t.Errorf("Expected file content %s, got %s", tt.mockContent, string(content))
				}

				// Verify no temp file remains
				tempPath := tt.localPath + ".tmp"
				if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
					t.Error("Temporary file should not exist after successful download")
				}
			}
		})
	}
}

func TestMockS3Client_GetObjectAttributes(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		key          string
		mockResponse *s3.HeadObjectOutput
		mockError    error
		expectedETag string
		expectedSize int64
		expectError  bool
	}{
		{
			name:   "successful get attributes",
			bucket: "test-bucket",
			key:    "test-file.gz",
			mockResponse: &s3.HeadObjectOutput{
				ETag:          aws.String("\"abc123\""),
				ContentLength: aws.Int64(1024),
			},
			expectedETag: "\"abc123\"",
			expectedSize: 1024,
			expectError:  false,
		},
		{
			name:        "API error",
			bucket:      "error-bucket",
			key:         "error-file.gz",
			mockError:   errors.New("object not found"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewMockS3Client()
			client.mockAPI.HeadObjectFunc = func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
				// Verify input parameters
				if aws.ToString(params.Bucket) != tt.bucket {
					t.Errorf("Expected bucket %s, got %s", tt.bucket, aws.ToString(params.Bucket))
				}
				
				if aws.ToString(params.Key) != tt.key {
					t.Errorf("Expected key %s, got %s", tt.key, aws.ToString(params.Key))
				}

				if tt.mockError != nil {
					return nil, tt.mockError
				}
				return tt.mockResponse, nil
			}

			ctx := context.Background()
			result, err := client.GetObjectAttributes(ctx, tt.bucket, tt.key)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.ETag != tt.expectedETag {
				t.Errorf("Expected ETag %s, got %s", tt.expectedETag, result.ETag)
			}

			if result.Size != tt.expectedSize {
				t.Errorf("Expected size %d, got %d", tt.expectedSize, result.Size)
			}
		})
	}
}

func TestS3Client_ContextCancellation(t *testing.T) {
	client := NewMockS3Client()
	
	// Test context cancellation for ListObjectsV2
	t.Run("ListObjectsV2 context cancellation", func(t *testing.T) {
		client.mockAPI.ListObjectsV2Func = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &s3.ListObjectsV2Output{}, nil
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := client.ListObjectsV2(ctx, "test-bucket", "", "")
		if err == nil {
			t.Error("Expected context cancellation error")
		}
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	// Test context timeout for DownloadFile
	t.Run("DownloadFile context timeout", func(t *testing.T) {
		client.mockAPI.GetObjectFunc = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &s3.GetObjectOutput{
					Body: newMockBody("test content"),
				}, nil
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		
		// Wait for timeout
		time.Sleep(1 * time.Millisecond)

		err := client.DownloadFile(ctx, "test-bucket", "test-key", "test-output/timeout-test.gz")
		if err == nil {
			t.Error("Expected context timeout error")
		}
	})
}

func TestS3Client_ErrorHandling(t *testing.T) {
	client := NewMockS3Client()

	// Test various AWS error scenarios
	t.Run("access denied error", func(t *testing.T) {
		client.mockAPI.ListObjectsV2Func = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("AccessDenied: Access Denied")
		}

		_, err := client.ListObjectsV2(context.Background(), "restricted-bucket", "", "")
		if err == nil {
			t.Error("Expected access denied error")
		}
		if !strings.Contains(err.Error(), "AccessDenied") {
			t.Errorf("Expected AccessDenied in error message, got: %v", err)
		}
	})

	t.Run("no such bucket error", func(t *testing.T) {
		client.mockAPI.ListObjectsV2Func = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, errors.New("NoSuchBucket: The specified bucket does not exist")
		}

		_, err := client.ListObjectsV2(context.Background(), "nonexistent-bucket", "", "")
		if err == nil {
			t.Error("Expected no such bucket error")
		}
		if !strings.Contains(err.Error(), "NoSuchBucket") {
			t.Errorf("Expected NoSuchBucket in error message, got: %v", err)
		}
	})
}