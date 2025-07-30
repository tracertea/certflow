package ctprocessor

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// S3FileDiscovery implements the FileDiscovery interface for S3 operations
type S3FileDiscovery struct {
	client S3Client
}

// NewS3FileDiscovery creates a new S3FileDiscovery instance
func NewS3FileDiscovery(client S3Client) *S3FileDiscovery {
	return &S3FileDiscovery{
		client: client,
	}
}

// ListObjects lists all objects in an S3 bucket with the given prefix, handling pagination
func (fd *S3FileDiscovery) ListObjects(bucket, prefix string) ([]S3Object, error) {
	var allObjects []S3Object
	var continuationToken string
	ctx := context.Background()

	for {
		output, err := fd.client.ListObjectsV2(ctx, bucket, prefix, continuationToken)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucket, prefix, err)
		}

		allObjects = append(allObjects, output.Objects...)

		// Check if there are more objects to fetch
		if !output.IsTruncated {
			break
		}

		continuationToken = output.NextContinuationToken
		if continuationToken == "" {
			break
		}
	}

	return allObjects, nil
}

// FilterObjects filters S3 objects based on the provided criteria
func (fd *S3FileDiscovery) FilterObjects(objects []S3Object, criteria FilterCriteria) []S3Object {
	var filtered []S3Object

	for _, obj := range objects {
		// Check file extension filter
		if len(criteria.Extensions) > 0 {
			hasValidExtension := false
			for _, ext := range criteria.Extensions {
				if strings.HasSuffix(strings.ToLower(obj.Key), strings.ToLower(ext)) {
					hasValidExtension = true
					break
				}
			}
			if !hasValidExtension {
				continue
			}
		}

		// Check minimum size filter
		if criteria.MinSize > 0 && obj.Size < criteria.MinSize {
			continue
		}

		// Check maximum size filter
		if criteria.MaxSize > 0 && obj.Size > criteria.MaxSize {
			continue
		}

		filtered = append(filtered, obj)
	}

	// Sort by key name if requested
	if criteria.SortByKey {
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].Key < filtered[j].Key
		})
	}

	return filtered
}

// ListAndFilterObjects is a convenience method that combines listing and filtering
func (fd *S3FileDiscovery) ListAndFilterObjects(bucket, prefix string, criteria FilterCriteria) ([]S3Object, error) {
	objects, err := fd.ListObjects(bucket, prefix)
	if err != nil {
		return nil, err
	}

	return fd.FilterObjects(objects, criteria), nil
}

// GetObjectCount returns the total number of objects matching the criteria
func (fd *S3FileDiscovery) GetObjectCount(bucket, prefix string, criteria FilterCriteria) (int, error) {
	objects, err := fd.ListAndFilterObjects(bucket, prefix, criteria)
	if err != nil {
		return 0, err
	}
	return len(objects), nil
}

// GetTotalSize returns the total size of objects matching the criteria
func (fd *S3FileDiscovery) GetTotalSize(bucket, prefix string, criteria FilterCriteria) (int64, error) {
	objects, err := fd.ListAndFilterObjects(bucket, prefix, criteria)
	if err != nil {
		return 0, err
	}

	var totalSize int64
	for _, obj := range objects {
		totalSize += obj.Size
	}

	return totalSize, nil
}

// ValidateFilterCriteria validates the filter criteria
func ValidateFilterCriteria(criteria FilterCriteria) error {
	// Validate size constraints
	if criteria.MinSize < 0 {
		return fmt.Errorf("minimum size cannot be negative")
	}

	if criteria.MaxSize < 0 {
		return fmt.Errorf("maximum size cannot be negative")
	}

	if criteria.MinSize > 0 && criteria.MaxSize > 0 && criteria.MinSize > criteria.MaxSize {
		return fmt.Errorf("minimum size cannot be greater than maximum size")
	}

	// Validate extensions
	for _, ext := range criteria.Extensions {
		if strings.TrimSpace(ext) == "" {
			return fmt.Errorf("extension cannot be empty")
		}
		if !strings.HasPrefix(ext, ".") {
			return fmt.Errorf("extension must start with a dot: %s", ext)
		}
	}

	return nil
}

// CreateGzipFilterCriteria creates filter criteria for gzip files with optional size constraints
func CreateGzipFilterCriteria(minSize, maxSize int64, sortByKey bool) FilterCriteria {
	return FilterCriteria{
		Extensions: []string{".gz"},
		MinSize:    minSize,
		MaxSize:    maxSize,
		SortByKey:  sortByKey,
	}
}

// GetFileExtension returns the file extension from an S3 object key
func GetFileExtension(key string) string {
	return strings.ToLower(filepath.Ext(key))
}

// IsGzipFile checks if an S3 object is a gzip file
func IsGzipFile(obj S3Object) bool {
	return GetFileExtension(obj.Key) == ".gz"
}