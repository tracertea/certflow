package ctprocessor

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// FileDiscoveryMockS3Client implements S3Client interface for testing FileDiscovery
type FileDiscoveryMockS3Client struct {
	objects               []S3Object
	listObjectsError      error
	continuationTokens    []string
	currentTokenIndex     int
	objectsPerPage        int
	downloadError         error
	objectAttributesError error
}

// NewFileDiscoveryMockS3Client creates a new mock S3 client for FileDiscovery testing
func NewFileDiscoveryMockS3Client() *FileDiscoveryMockS3Client {
	return &FileDiscoveryMockS3Client{
		objects:        []S3Object{},
		objectsPerPage: 1000, // Default to return all objects in one page
	}
}

// SetObjects sets the objects that the mock client will return
func (m *FileDiscoveryMockS3Client) SetObjects(objects []S3Object) {
	m.objects = objects
}

// SetListObjectsError sets an error to be returned by ListObjectsV2
func (m *FileDiscoveryMockS3Client) SetListObjectsError(err error) {
	m.listObjectsError = err
}

// SetPagination configures pagination for testing
func (m *FileDiscoveryMockS3Client) SetPagination(objectsPerPage int, tokens []string) {
	m.objectsPerPage = objectsPerPage
	m.continuationTokens = tokens
	m.currentTokenIndex = 0
}

// ListObjectsV2 implements the S3Client interface
func (m *FileDiscoveryMockS3Client) ListObjectsV2(ctx context.Context, bucket, prefix string, continuationToken string) (*ListObjectsV2Output, error) {
	if m.listObjectsError != nil {
		return nil, m.listObjectsError
	}

	// Filter objects by prefix
	var filteredObjects []S3Object
	for _, obj := range m.objects {
		if prefix == "" || len(obj.Key) >= len(prefix) && obj.Key[:len(prefix)] == prefix {
			filteredObjects = append(filteredObjects, obj)
		}
	}

	// Handle pagination
	startIndex := 0
	if continuationToken != "" {
		// Find the starting index based on continuation token
		for i, token := range m.continuationTokens {
			if token == continuationToken {
				startIndex = (i + 1) * m.objectsPerPage
				break
			}
		}
	}

	endIndex := startIndex + m.objectsPerPage
	if endIndex > len(filteredObjects) {
		endIndex = len(filteredObjects)
	}

	var pageObjects []S3Object
	if startIndex < len(filteredObjects) {
		pageObjects = filteredObjects[startIndex:endIndex]
	}

	output := &ListObjectsV2Output{
		Objects:     pageObjects,
		IsTruncated: endIndex < len(filteredObjects),
	}

	// Set next continuation token if there are more objects
	if output.IsTruncated && m.currentTokenIndex < len(m.continuationTokens) {
		output.NextContinuationToken = m.continuationTokens[m.currentTokenIndex]
		m.currentTokenIndex++
	}

	return output, nil
}

// DownloadFile implements the S3Client interface
func (m *FileDiscoveryMockS3Client) DownloadFile(ctx context.Context, bucket, key, localPath string) error {
	return m.downloadError
}

// GetObjectAttributes implements the S3Client interface
func (m *FileDiscoveryMockS3Client) GetObjectAttributes(ctx context.Context, bucket, key string) (*ObjectAttributes, error) {
	if m.objectAttributesError != nil {
		return nil, m.objectAttributesError
	}

	// Find the object and return its attributes
	for _, obj := range m.objects {
		if obj.Key == key {
			return &ObjectAttributes{
				ETag: obj.ETag,
				Size: obj.Size,
			}, nil
		}
	}

	return nil, fmt.Errorf("object not found: %s", key)
}

// GetObject implements the S3Client interface
func (m *FileDiscoveryMockS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	// For FileDiscovery testing, this method is not used, so return a simple implementation
	return io.NopCloser(strings.NewReader("mock data")), nil
}

// Helper function to create test S3 objects
func createTestS3Objects() []S3Object {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return []S3Object{
		{
			Key:          "data/file1.gz",
			Size:         1024,
			ETag:         "etag1",
			LastModified: baseTime,
		},
		{
			Key:          "data/file2.gz",
			Size:         2048,
			ETag:         "etag2",
			LastModified: baseTime.Add(time.Hour),
		},
		{
			Key:          "data/file3.txt",
			Size:         512,
			ETag:         "etag3",
			LastModified: baseTime.Add(2 * time.Hour),
		},
		{
			Key:          "logs/access.log",
			Size:         4096,
			ETag:         "etag4",
			LastModified: baseTime.Add(3 * time.Hour),
		},
		{
			Key:          "data/large_file.gz",
			Size:         10240,
			ETag:         "etag5",
			LastModified: baseTime.Add(4 * time.Hour),
		},
	}
}

func TestNewS3FileDiscovery(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	discovery := NewS3FileDiscovery(mockClient)

	if discovery == nil {
		t.Fatal("Expected non-nil S3FileDiscovery")
	}

	if discovery.client != mockClient {
		t.Error("Expected client to be set correctly")
	}
}

func TestListObjects_Success(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	discovery := NewS3FileDiscovery(mockClient)

	objects, err := discovery.ListObjects("test-bucket", "data/")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should return objects with "data/" prefix
	expectedCount := 4 // file1.gz, file2.gz, file3.txt, large_file.gz
	if len(objects) != expectedCount {
		t.Errorf("Expected %d objects, got %d", expectedCount, len(objects))
	}

	// Verify the objects are correctly filtered by prefix
	for _, obj := range objects {
		if len(obj.Key) < 5 || obj.Key[:5] != "data/" {
			t.Errorf("Expected object key to start with 'data/', got: %s", obj.Key)
		}
	}
}

func TestListObjects_EmptyPrefix(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	discovery := NewS3FileDiscovery(mockClient)

	objects, err := discovery.ListObjects("test-bucket", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should return all objects when prefix is empty
	if len(objects) != len(testObjects) {
		t.Errorf("Expected %d objects, got %d", len(testObjects), len(objects))
	}
}

func TestListObjects_Pagination(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	// Set up pagination: 2 objects per page
	tokens := []string{"token1", "token2"}
	mockClient.SetPagination(2, tokens)

	discovery := NewS3FileDiscovery(mockClient)

	objects, err := discovery.ListObjects("test-bucket", "")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should return all objects despite pagination
	if len(objects) != len(testObjects) {
		t.Errorf("Expected %d objects, got %d", len(testObjects), len(objects))
	}
}

func TestListObjects_Error(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	expectedError := fmt.Errorf("S3 access denied")
	mockClient.SetListObjectsError(expectedError)

	discovery := NewS3FileDiscovery(mockClient)

	objects, err := discovery.ListObjects("test-bucket", "data/")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if objects != nil {
		t.Error("Expected nil objects on error")
	}

	if !contains(err.Error(), "failed to list objects") {
		t.Errorf("Expected error message to contain 'failed to list objects', got: %v", err)
	}
}

func TestFilterObjects_ByExtension(t *testing.T) {
	discovery := NewS3FileDiscovery(nil) // Client not needed for filtering
	testObjects := createTestS3Objects()

	criteria := FilterCriteria{
		Extensions: []string{".gz"},
	}

	filtered := discovery.FilterObjects(testObjects, criteria)

	// Should return only .gz files
	expectedCount := 3 // file1.gz, file2.gz, large_file.gz
	if len(filtered) != expectedCount {
		t.Errorf("Expected %d filtered objects, got %d", expectedCount, len(filtered))
	}

	for _, obj := range filtered {
		if !IsGzipFile(obj) {
			t.Errorf("Expected only .gz files, got: %s", obj.Key)
		}
	}
}

func TestFilterObjects_BySize(t *testing.T) {
	discovery := NewS3FileDiscovery(nil)
	testObjects := createTestS3Objects()

	criteria := FilterCriteria{
		MinSize: 1000,
		MaxSize: 5000,
	}

	filtered := discovery.FilterObjects(testObjects, criteria)

	// Should return objects with size between 1000 and 5000
	expectedCount := 3 // file1.gz (1024), file2.gz (2048), logs/access.log (4096)
	if len(filtered) != expectedCount {
		t.Errorf("Expected %d filtered objects, got %d", expectedCount, len(filtered))
	}

	for _, obj := range filtered {
		if obj.Size < 1000 || obj.Size > 5000 {
			t.Errorf("Expected object size between 1000 and 5000, got: %d for %s", obj.Size, obj.Key)
		}
	}
}

func TestFilterObjects_SortByKey(t *testing.T) {
	discovery := NewS3FileDiscovery(nil)
	testObjects := createTestS3Objects()

	criteria := FilterCriteria{
		SortByKey: true,
	}

	filtered := discovery.FilterObjects(testObjects, criteria)

	// Should return all objects sorted by key
	if len(filtered) != len(testObjects) {
		t.Errorf("Expected %d filtered objects, got %d", len(testObjects), len(filtered))
	}

	// Verify sorting
	for i := 1; i < len(filtered); i++ {
		if filtered[i-1].Key > filtered[i].Key {
			t.Errorf("Objects not sorted by key: %s > %s", filtered[i-1].Key, filtered[i].Key)
		}
	}
}

func TestFilterObjects_CombinedCriteria(t *testing.T) {
	discovery := NewS3FileDiscovery(nil)
	testObjects := createTestS3Objects()

	criteria := FilterCriteria{
		Extensions: []string{".gz"},
		MinSize:    1500,
		SortByKey:  true,
	}

	filtered := discovery.FilterObjects(testObjects, criteria)

	// Should return .gz files with size >= 1500, sorted by key
	expectedCount := 2 // file2.gz (2048), large_file.gz (10240)
	if len(filtered) != expectedCount {
		t.Errorf("Expected %d filtered objects, got %d", expectedCount, len(filtered))
	}

	// Verify all criteria are met
	for i, obj := range filtered {
		if !IsGzipFile(obj) {
			t.Errorf("Expected only .gz files, got: %s", obj.Key)
		}
		if obj.Size < 1500 {
			t.Errorf("Expected size >= 1500, got: %d for %s", obj.Size, obj.Key)
		}
		if i > 0 && filtered[i-1].Key > obj.Key {
			t.Errorf("Objects not sorted by key: %s > %s", filtered[i-1].Key, obj.Key)
		}
	}
}

func TestListAndFilterObjects(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	discovery := NewS3FileDiscovery(mockClient)

	criteria := FilterCriteria{
		Extensions: []string{".gz"},
		SortByKey:  true,
	}

	objects, err := discovery.ListAndFilterObjects("test-bucket", "data/", criteria)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should return .gz files from data/ prefix, sorted by key
	expectedCount := 3 // file1.gz, file2.gz, large_file.gz
	if len(objects) != expectedCount {
		t.Errorf("Expected %d objects, got %d", expectedCount, len(objects))
	}

	// Verify all are .gz files and sorted
	for i, obj := range objects {
		if !IsGzipFile(obj) {
			t.Errorf("Expected only .gz files, got: %s", obj.Key)
		}
		if i > 0 && objects[i-1].Key > obj.Key {
			t.Errorf("Objects not sorted by key: %s > %s", objects[i-1].Key, obj.Key)
		}
	}
}

func TestGetObjectCount(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	discovery := NewS3FileDiscovery(mockClient)

	criteria := FilterCriteria{
		Extensions: []string{".gz"},
	}

	count, err := discovery.GetObjectCount("test-bucket", "data/", criteria)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	expectedCount := 3 // file1.gz, file2.gz, large_file.gz
	if count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, count)
	}
}

func TestGetTotalSize(t *testing.T) {
	mockClient := NewFileDiscoveryMockS3Client()
	testObjects := createTestS3Objects()
	mockClient.SetObjects(testObjects)

	discovery := NewS3FileDiscovery(mockClient)

	criteria := FilterCriteria{
		Extensions: []string{".gz"},
	}

	totalSize, err := discovery.GetTotalSize("test-bucket", "data/", criteria)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// file1.gz (1024) + file2.gz (2048) + large_file.gz (10240) = 13312
	expectedSize := int64(13312)
	if totalSize != expectedSize {
		t.Errorf("Expected total size %d, got %d", expectedSize, totalSize)
	}
}

func TestValidateFilterCriteria(t *testing.T) {
	tests := []struct {
		name        string
		criteria    FilterCriteria
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid criteria",
			criteria: FilterCriteria{
				Extensions: []string{".gz", ".txt"},
				MinSize:    100,
				MaxSize:    1000,
				SortByKey:  true,
			},
			expectError: false,
		},
		{
			name: "Negative min size",
			criteria: FilterCriteria{
				MinSize: -1,
			},
			expectError: true,
			errorMsg:    "minimum size cannot be negative",
		},
		{
			name: "Negative max size",
			criteria: FilterCriteria{
				MaxSize: -1,
			},
			expectError: true,
			errorMsg:    "maximum size cannot be negative",
		},
		{
			name: "Min size greater than max size",
			criteria: FilterCriteria{
				MinSize: 1000,
				MaxSize: 500,
			},
			expectError: true,
			errorMsg:    "minimum size cannot be greater than maximum size",
		},
		{
			name: "Empty extension",
			criteria: FilterCriteria{
				Extensions: []string{""},
			},
			expectError: true,
			errorMsg:    "extension cannot be empty",
		},
		{
			name: "Extension without dot",
			criteria: FilterCriteria{
				Extensions: []string{"gz"},
			},
			expectError: true,
			errorMsg:    "extension must start with a dot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFilterCriteria(tt.criteria)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				} else if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}
			}
		})
	}
}

func TestCreateGzipFilterCriteria(t *testing.T) {
	criteria := CreateGzipFilterCriteria(1000, 5000, true)

	if len(criteria.Extensions) != 1 || criteria.Extensions[0] != ".gz" {
		t.Errorf("Expected extensions ['.gz'], got: %v", criteria.Extensions)
	}

	if criteria.MinSize != 1000 {
		t.Errorf("Expected MinSize 1000, got: %d", criteria.MinSize)
	}

	if criteria.MaxSize != 5000 {
		t.Errorf("Expected MaxSize 5000, got: %d", criteria.MaxSize)
	}

	if !criteria.SortByKey {
		t.Error("Expected SortByKey to be true")
	}
}

func TestGetFileExtension(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"file.gz", ".gz"},
		{"path/to/file.txt", ".txt"},
		{"file.TAR.GZ", ".gz"},
		{"file", ""},
		{"file.", "."},
		{".hidden", ".hidden"},
		{"path/file.backup.gz", ".gz"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := GetFileExtension(tt.key)
			if result != tt.expected {
				t.Errorf("Expected extension '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestIsGzipFile(t *testing.T) {
	tests := []struct {
		obj      S3Object
		expected bool
	}{
		{S3Object{Key: "file.gz"}, true},
		{S3Object{Key: "file.GZ"}, true},
		{S3Object{Key: "file.txt"}, false},
		{S3Object{Key: "file"}, false},
		{S3Object{Key: "path/to/file.gz"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.obj.Key, func(t *testing.T) {
			result := IsGzipFile(tt.obj)
			if result != tt.expected {
				t.Errorf("Expected IsGzipFile(%s) to be %v, got %v", tt.obj.Key, tt.expected, result)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		 containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}