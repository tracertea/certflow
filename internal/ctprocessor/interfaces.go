package ctprocessor

// No imports needed for interface definitions

// FileProcessor handles the main processing workflow
type FileProcessor interface {
	ProcessFile(config Config) error
	ProcessBatch(inputPaths []string, config Config) error
}

// CertificateParser is implemented as a concrete struct in certificate_parser.go

// OutputWriter handles writing results in different formats
type OutputWriter interface {
	WriteMapping(mapping DomainMapping) error
	WriteBatch(mappings []DomainMapping) error
	Close() error
}

// FileDiscovery handles S3 file listing and filtering
type FileDiscovery interface {
	ListObjects(bucket, prefix string) ([]S3Object, error)
	FilterObjects(objects []S3Object, criteria FilterCriteria) []S3Object
}

// StateTracker handles processing state management for S3 operations
type StateTracker interface {
	LoadState() error
	IsProcessed(key string, etag string) bool
	MarkProcessed(key string, etag string, size int64) error
	MarkFailed(key string, etag string, errorMsg string) error
	SaveState() error
	GetProcessedCount() int
	GetFailedCount() int
}

// DownloadManager handles parallel file downloads from S3
type DownloadManager interface {
	DownloadFiles(objects []S3Object, config DownloadConfig) <-chan DownloadResult
	GetDownloadStats() DownloadStats
}

// S3Manager handles the main S3 processing workflow orchestration
type S3Manager interface {
	ProcessS3Bucket(config S3Config) error
	ListFiles(bucket, prefix string) ([]S3Object, error)
	GetProcessingStats() S3ProcessingStats
}



// Core data structures are defined in models.go