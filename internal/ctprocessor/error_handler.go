package ctprocessor

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// ErrorType represents different categories of errors
type ErrorType int

const (
	ErrorTypeFile ErrorType = iota
	ErrorTypeParsing
	ErrorTypeProcessing
	ErrorTypeOutput
	ErrorTypeNetwork
	ErrorTypeMemory
	ErrorTypeS3Access
	ErrorTypeS3Download
	ErrorTypeS3State
	ErrorTypeUnknown
)

// String returns the string representation of ErrorType
func (et ErrorType) String() string {
	switch et {
	case ErrorTypeFile:
		return "FILE"
	case ErrorTypeParsing:
		return "PARSING"
	case ErrorTypeProcessing:
		return "PROCESSING"
	case ErrorTypeOutput:
		return "OUTPUT"
	case ErrorTypeNetwork:
		return "NETWORK"
	case ErrorTypeMemory:
		return "MEMORY"
	case ErrorTypeS3Access:
		return "S3_ACCESS"
	case ErrorTypeS3Download:
		return "S3_DOWNLOAD"
	case ErrorTypeS3State:
		return "S3_STATE"
	default:
		return "UNKNOWN"
	}
}

// ProcessingError represents a detailed error with context
type ProcessingError struct {
	Type        ErrorType
	Message     string
	Context     string
	SequenceNum int64
	Timestamp   time.Time
	Cause       error
}

// Error implements the error interface
func (pe *ProcessingError) Error() string {
	if pe.SequenceNum >= 0 {
		return fmt.Sprintf("[%s] seq:%d %s: %s", pe.Type, pe.SequenceNum, pe.Context, pe.Message)
	}
	return fmt.Sprintf("[%s] %s: %s", pe.Type, pe.Context, pe.Message)
}

// Unwrap returns the underlying cause error
func (pe *ProcessingError) Unwrap() error {
	return pe.Cause
}

// ErrorHandler manages error handling, logging, and recovery
type ErrorHandler struct {
	logger       *log.Logger
	stats        *ProcessingStats
	maxErrors    int64
	errorCount   int64
	errorsByType map[ErrorType]int64
	recentErrors []ProcessingError
	maxRecent    int
	mutex        sync.RWMutex
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(logger *log.Logger, stats *ProcessingStats, maxErrors int64) *ErrorHandler {
	if logger == nil {
		logger = log.Default()
	}
	if maxErrors <= 0 {
		maxErrors = 1000 // Default max errors
	}

	return &ErrorHandler{
		logger:       logger,
		stats:        stats,
		maxErrors:    maxErrors,
		errorsByType: make(map[ErrorType]int64),
		recentErrors: make([]ProcessingError, 0),
		maxRecent:    100, // Keep last 100 errors
	}
}

// HandleError processes an error and determines if processing should continue
func (eh *ErrorHandler) HandleError(err error, errorType ErrorType, context string, sequenceNum int64) bool {
	eh.mutex.Lock()
	defer eh.mutex.Unlock()

	// Create processing error
	procError := ProcessingError{
		Type:        errorType,
		Message:     err.Error(),
		Context:     context,
		SequenceNum: sequenceNum,
		Timestamp:   time.Now(),
		Cause:       err,
	}

	// Update statistics
	eh.errorCount++
	eh.errorsByType[errorType]++
	if eh.stats != nil {
		eh.stats.AddError()
	}

	// Add to recent errors (keep only last maxRecent)
	eh.recentErrors = append(eh.recentErrors, procError)
	if len(eh.recentErrors) > eh.maxRecent {
		eh.recentErrors = eh.recentErrors[1:]
	}

	// Log the error
	eh.logError(procError)

	// Check if we should continue processing
	shouldContinue := eh.errorCount < eh.maxErrors
	if !shouldContinue {
		eh.logger.Printf("ERROR: Maximum error count (%d) reached, stopping processing", eh.maxErrors)
	}

	return shouldContinue
}

// logError logs an error with appropriate level based on type and frequency
func (eh *ErrorHandler) logError(procError ProcessingError) {
	// Determine log level based on error type and frequency
	errorTypeCount := eh.errorsByType[procError.Type]
	
	switch procError.Type {
	case ErrorTypeFile, ErrorTypeOutput:
		// File and output errors are always critical
		eh.logger.Printf("ERROR: %s", procError.Error())
	case ErrorTypeMemory, ErrorTypeNetwork:
		// Memory and network errors are critical
		eh.logger.Printf("CRITICAL: %s", procError.Error())
	case ErrorTypeParsing:
		// Parsing errors are common, log less frequently
		if errorTypeCount <= 10 || errorTypeCount%100 == 0 {
			eh.logger.Printf("WARNING: %s (total parsing errors: %d)", procError.Error(), errorTypeCount)
		}
	case ErrorTypeProcessing:
		// Processing errors are warnings
		if errorTypeCount <= 5 || errorTypeCount%50 == 0 {
			eh.logger.Printf("WARNING: %s (total processing errors: %d)", procError.Error(), errorTypeCount)
		}
	default:
		eh.logger.Printf("ERROR: %s", procError.Error())
	}
}

// GetErrorSummary returns a summary of all errors encountered
func (eh *ErrorHandler) GetErrorSummary() ErrorSummary {
	eh.mutex.RLock()
	defer eh.mutex.RUnlock()

	summary := ErrorSummary{
		TotalErrors:  eh.errorCount,
		ErrorsByType: make(map[ErrorType]int64),
		RecentErrors: make([]ProcessingError, len(eh.recentErrors)),
	}

	// Copy error counts by type
	for errorType, count := range eh.errorsByType {
		summary.ErrorsByType[errorType] = count
	}

	// Copy recent errors
	copy(summary.RecentErrors, eh.recentErrors)

	return summary
}

// GetErrorCount returns the total error count
func (eh *ErrorHandler) GetErrorCount() int64 {
	eh.mutex.RLock()
	defer eh.mutex.RUnlock()
	return eh.errorCount
}

// GetErrorCountByType returns the error count for a specific type
func (eh *ErrorHandler) GetErrorCountByType(errorType ErrorType) int64 {
	eh.mutex.RLock()
	defer eh.mutex.RUnlock()
	return eh.errorsByType[errorType]
}

// ShouldContinue checks if processing should continue based on error count
func (eh *ErrorHandler) ShouldContinue() bool {
	eh.mutex.RLock()
	defer eh.mutex.RUnlock()
	return eh.errorCount < eh.maxErrors
}

// Reset resets the error handler state
func (eh *ErrorHandler) Reset() {
	eh.mutex.Lock()
	defer eh.mutex.Unlock()

	eh.errorCount = 0
	eh.errorsByType = make(map[ErrorType]int64)
	eh.recentErrors = make([]ProcessingError, 0)
}

// ErrorSummary provides a summary of errors encountered during processing
type ErrorSummary struct {
	TotalErrors  int64
	ErrorsByType map[ErrorType]int64
	RecentErrors []ProcessingError
}

// String returns a string representation of the error summary
func (es *ErrorSummary) String() string {
	if es.TotalErrors == 0 {
		return "No errors encountered"
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("Total errors: %d", es.TotalErrors))

	// Add breakdown by type
	if len(es.ErrorsByType) > 0 {
		var typeBreakdown []string
		for errorType, count := range es.ErrorsByType {
			if count > 0 {
				typeBreakdown = append(typeBreakdown, fmt.Sprintf("%s: %d", errorType, count))
			}
		}
		if len(typeBreakdown) > 0 {
			parts = append(parts, "Breakdown: "+strings.Join(typeBreakdown, ", "))
		}
	}

	return strings.Join(parts, "; ")
}

// ErrorClassifier helps classify errors into appropriate types
type ErrorClassifier struct{}

// NewErrorClassifier creates a new error classifier
func NewErrorClassifier() *ErrorClassifier {
	return &ErrorClassifier{}
}

// ClassifyError determines the error type based on the error message and context
func (ec *ErrorClassifier) ClassifyError(err error, context string) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}

	errMsg := strings.ToLower(err.Error())
	contextLower := strings.ToLower(context)

	// S3-specific errors (check first for S3 operations)
	if strings.Contains(contextLower, "s3") || strings.Contains(errMsg, "s3") {
		if strings.Contains(errMsg, "access denied") ||
			strings.Contains(errMsg, "forbidden") ||
			strings.Contains(errMsg, "invalid access key") ||
			strings.Contains(errMsg, "no such bucket") ||
			strings.Contains(contextLower, "list") ||
			strings.Contains(contextLower, "access") {
			return ErrorTypeS3Access
		}
		if strings.Contains(errMsg, "no such key") ||
			strings.Contains(contextLower, "download") ||
			strings.Contains(contextLower, "get") {
			return ErrorTypeS3Download
		}
		if strings.Contains(contextLower, "state") {
			return ErrorTypeS3State
		}
		// Default S3 error to access type
		return ErrorTypeS3Access
	}

	// Memory errors (check first as they're specific)
	if strings.Contains(errMsg, "out of memory") ||
		strings.Contains(errMsg, "allocation failed") ||
		(strings.Contains(errMsg, "memory") && !strings.Contains(errMsg, "file")) {
		return ErrorTypeMemory
	}

	// Network errors (check before file errors)
	if strings.Contains(errMsg, "network") ||
		strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "dns") {
		return ErrorTypeNetwork
	}

	// File-related errors
	if strings.Contains(errMsg, "no such file") ||
		strings.Contains(errMsg, "permission denied") ||
		strings.Contains(errMsg, "file not found") ||
		strings.Contains(errMsg, "cannot open") ||
		strings.Contains(contextLower, "file") {
		return ErrorTypeFile
	}

	// Parsing errors
	if strings.Contains(errMsg, "parse") ||
		strings.Contains(errMsg, "decode") ||
		strings.Contains(errMsg, "invalid") ||
		strings.Contains(errMsg, "malformed") ||
		strings.Contains(errMsg, "base64") ||
		strings.Contains(errMsg, "certificate") ||
		strings.Contains(contextLower, "parse") ||
		strings.Contains(contextLower, "decode") {
		return ErrorTypeParsing
	}

	// Output errors
	if strings.Contains(errMsg, "write") ||
		strings.Contains(errMsg, "output") ||
		strings.Contains(contextLower, "write") ||
		strings.Contains(contextLower, "output") {
		return ErrorTypeOutput
	}

	// Processing errors (default for certificate processing issues)
	if strings.Contains(contextLower, "process") ||
		strings.Contains(contextLower, "extract") ||
		strings.Contains(contextLower, "mapping") {
		return ErrorTypeProcessing
	}

	return ErrorTypeUnknown
}

// RecoveryStrategy defines how to handle different types of errors
type RecoveryStrategy struct {
	classifier *ErrorClassifier
}

// NewRecoveryStrategy creates a new recovery strategy
func NewRecoveryStrategy() *RecoveryStrategy {
	return &RecoveryStrategy{
		classifier: NewErrorClassifier(),
	}
}

// ShouldRetry determines if an operation should be retried based on error type
func (rs *RecoveryStrategy) ShouldRetry(err error, context string, attemptCount int) bool {
	errorType := rs.classifier.ClassifyError(err, context)
	maxRetries := rs.getMaxRetries(errorType)
	
	return attemptCount < maxRetries
}

// getMaxRetries returns the maximum number of retries for each error type
func (rs *RecoveryStrategy) getMaxRetries(errorType ErrorType) int {
	switch errorType {
	case ErrorTypeNetwork:
		return 3 // Network errors might be transient
	case ErrorTypeMemory:
		return 1 // Memory errors rarely benefit from retries
	case ErrorTypeFile:
		return 2 // File errors might be transient (e.g., temporary locks)
	case ErrorTypeOutput:
		return 2 // Output errors might be transient
	case ErrorTypeParsing:
		return 0 // Parsing errors are usually permanent
	case ErrorTypeProcessing:
		return 1 // Processing errors might benefit from one retry
	case ErrorTypeS3Access:
		return 3 // S3 access errors might be transient
	case ErrorTypeS3Download:
		return 10 // Download errors should be retried more aggressively
	case ErrorTypeS3State:
		return 2 // State errors might be transient
	default:
		return 1 // Default retry once
	}
}

// GetRecoveryAction suggests an action to take for error recovery
func (rs *RecoveryStrategy) GetRecoveryAction(err error, context string) RecoveryAction {
	errorType := rs.classifier.ClassifyError(err, context)
	
	switch errorType {
	case ErrorTypeMemory:
		return RecoveryActionReduceLoad
	case ErrorTypeNetwork:
		return RecoveryActionRetryWithDelay
	case ErrorTypeFile:
		return RecoveryActionRetry
	case ErrorTypeOutput:
		return RecoveryActionRetry
	case ErrorTypeParsing:
		return RecoveryActionSkip
	case ErrorTypeProcessing:
		return RecoveryActionSkip
	case ErrorTypeS3Access:
		return RecoveryActionRetryWithDelay
	case ErrorTypeS3Download:
		return RecoveryActionRetryWithDelay
	case ErrorTypeS3State:
		return RecoveryActionRetry
	default:
		return RecoveryActionSkip
	}
}

// RecoveryAction represents different recovery actions
type RecoveryAction int

const (
	RecoveryActionSkip RecoveryAction = iota
	RecoveryActionRetry
	RecoveryActionRetryWithDelay
	RecoveryActionReduceLoad
	RecoveryActionStop
	RecoveryActionWait
)

// S3ErrorHandler extends ErrorHandler with S3-specific error handling
type S3ErrorHandler interface {
	HandleS3Error(err error, operation string) ErrorAction
	HandleDownloadError(err error, object S3Object, attempt int) ErrorAction
	HandleProcessingError(err error, filePath string) ErrorAction
	HandleStateError(err error) ErrorAction
	GetRetryDelay(attempt int, errorType ErrorType) time.Duration
	IsRetryableError(err error, errorType ErrorType) bool
	GetTroubleshootingMessage(err error, errorType ErrorType) string
}

// ErrorAction represents the action to take for an error
type ErrorAction int

const (
	ErrorActionRetry ErrorAction = iota
	ErrorActionSkip
	ErrorActionFail
	ErrorActionWait
)

// String returns the string representation of ErrorAction
func (ea ErrorAction) String() string {
	switch ea {
	case ErrorActionRetry:
		return "RETRY"
	case ErrorActionSkip:
		return "SKIP"
	case ErrorActionFail:
		return "FAIL"
	case ErrorActionWait:
		return "WAIT"
	default:
		return "UNKNOWN"
	}
}

// RetryConfig defines retry behavior for different error types
type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	JitterEnabled   bool
	RetryableErrors []string
}

// String returns the string representation of RecoveryAction
func (ra RecoveryAction) String() string {
	switch ra {
	case RecoveryActionSkip:
		return "SKIP"
	case RecoveryActionRetry:
		return "RETRY"
	case RecoveryActionRetryWithDelay:
		return "RETRY_WITH_DELAY"
	case RecoveryActionReduceLoad:
		return "REDUCE_LOAD"
	case RecoveryActionStop:
		return "STOP"
	case RecoveryActionWait:
		return "WAIT"
	default:
		return "UNKNOWN"
	}
}

// S3ErrorHandlerImpl implements S3ErrorHandler with comprehensive S3 error handling
type S3ErrorHandlerImpl struct {
	*ErrorHandler
	classifier   *ErrorClassifier
	retryConfigs map[ErrorType]RetryConfig
}

// NewS3ErrorHandler creates a new S3-specific error handler
func NewS3ErrorHandler(logger *log.Logger, stats *ProcessingStats, maxErrors int64) *S3ErrorHandlerImpl {
	baseHandler := NewErrorHandler(logger, stats, maxErrors)
	
	return &S3ErrorHandlerImpl{
		ErrorHandler: baseHandler,
		classifier:   NewErrorClassifier(),
		retryConfigs: map[ErrorType]RetryConfig{
			ErrorTypeS3Access: {
				MaxRetries:      3,
				InitialDelay:    1 * time.Second,
				MaxDelay:        30 * time.Second,
				BackoffFactor:   2.0,
				JitterEnabled:   true,
				RetryableErrors: []string{"RequestTimeout", "ServiceUnavailable", "SlowDown", "InternalError"},
			},
			ErrorTypeS3Download: {
				MaxRetries:      10,
				InitialDelay:    500 * time.Millisecond,
				MaxDelay:        60 * time.Second,
				BackoffFactor:   2.0,
				JitterEnabled:   true,
				RetryableErrors: []string{"RequestTimeout", "ServiceUnavailable", "SlowDown", "InternalError", "connection", "timeout"},
			},
			ErrorTypeS3State: {
				MaxRetries:      2,
				InitialDelay:    100 * time.Millisecond,
				MaxDelay:        5 * time.Second,
				BackoffFactor:   2.0,
				JitterEnabled:   false,
				RetryableErrors: []string{"permission", "lock", "busy"},
			},
			ErrorTypeNetwork: {
				MaxRetries:      5,
				InitialDelay:    1 * time.Second,
				MaxDelay:        30 * time.Second,
				BackoffFactor:   2.0,
				JitterEnabled:   true,
				RetryableErrors: []string{"timeout", "connection", "dns", "network"},
			},
		},
	}
}

// HandleS3Error handles S3 access errors with appropriate classification and retry logic
func (s3eh *S3ErrorHandlerImpl) HandleS3Error(err error, operation string) ErrorAction {
	if err == nil {
		return ErrorActionRetry
	}

	errorType := s3eh.classifyS3Error(err, operation)
	
	// Log the error with context
	s3eh.HandleError(err, errorType, operation, -1)
	
	// Determine action based on error type and content
	if s3eh.isRateLimitError(err) {
		return ErrorActionWait
	}
	
	if s3eh.isAuthenticationError(err) {
		return ErrorActionFail
	}
	
	if strings.Contains(strings.ToLower(err.Error()), "nosuchbucket") {
		return ErrorActionFail
	}
	
	if s3eh.isRetryableS3Error(err, errorType) {
		return ErrorActionRetry
	}
	
	return ErrorActionSkip
}

// HandleDownloadError handles download-specific errors with retry logic
func (s3eh *S3ErrorHandlerImpl) HandleDownloadError(err error, object S3Object, attempt int) ErrorAction {
	if err == nil {
		return ErrorActionRetry
	}

	errorType := ErrorTypeS3Download
	context := fmt.Sprintf("downloading %s (attempt %d)", object.Key, attempt)
	
	// Log the error
	s3eh.HandleError(err, errorType, context, -1)
	
	// Check if we should retry based on attempt count and error type
	config := s3eh.retryConfigs[errorType]
	if attempt >= config.MaxRetries {
		return ErrorActionFail
	}
	
	if s3eh.isRetryableS3Error(err, errorType) {
		if s3eh.isRateLimitError(err) {
			return ErrorActionWait
		}
		return ErrorActionRetry
	}
	
	return ErrorActionSkip
}

// HandleProcessingError handles processing errors for downloaded files
func (s3eh *S3ErrorHandlerImpl) HandleProcessingError(err error, filePath string) ErrorAction {
	if err == nil {
		return ErrorActionRetry
	}

	errorType := s3eh.classifier.ClassifyError(err, "processing")
	context := fmt.Sprintf("processing file %s", filePath)
	
	// Log the error
	s3eh.HandleError(err, errorType, context, -1)
	
	// Processing errors are usually not retryable
	switch errorType {
	case ErrorTypeMemory:
		return ErrorActionWait // Wait and potentially reduce load
	case ErrorTypeFile:
		return ErrorActionRetry // File might be temporarily locked
	case ErrorTypeProcessing:
		// Check if it's a file lock error specifically
		if strings.Contains(strings.ToLower(err.Error()), "lock") {
			return ErrorActionRetry
		}
		return ErrorActionSkip // Skip corrupted or unparseable files
	default:
		return ErrorActionSkip // Skip corrupted or unparseable files
	}
}

// HandleStateError handles state management errors
func (s3eh *S3ErrorHandlerImpl) HandleStateError(err error) ErrorAction {
	if err == nil {
		return ErrorActionRetry
	}

	errorType := ErrorTypeS3State
	context := "state management"
	
	// Log the error
	s3eh.HandleError(err, errorType, context, -1)
	
	if s3eh.isRetryableS3Error(err, errorType) {
		return ErrorActionRetry
	}
	
	// State errors might be critical
	if strings.Contains(strings.ToLower(err.Error()), "corruption") {
		return ErrorActionFail
	}
	
	return ErrorActionSkip
}

// GetRetryDelay calculates the delay before retrying based on attempt count and error type
func (s3eh *S3ErrorHandlerImpl) GetRetryDelay(attempt int, errorType ErrorType) time.Duration {
	config, exists := s3eh.retryConfigs[errorType]
	if !exists {
		// Default retry config
		config = RetryConfig{
			InitialDelay:  1 * time.Second,
			MaxDelay:      30 * time.Second,
			BackoffFactor: 2.0,
			JitterEnabled: true,
		}
	}
	
	// Calculate exponential backoff delay
	delay := time.Duration(float64(config.InitialDelay) * math.Pow(config.BackoffFactor, float64(attempt)))
	
	// Cap at maximum delay
	if delay > config.MaxDelay {
		delay = config.MaxDelay
	}
	
	// Add jitter if enabled
	if config.JitterEnabled {
		jitter := time.Duration(rand.Float64() * float64(delay) * 0.1) // 10% jitter
		delay += jitter
	}
	
	return delay
}

// IsRetryableError determines if an error is retryable based on error type and content
func (s3eh *S3ErrorHandlerImpl) IsRetryableError(err error, errorType ErrorType) bool {
	return s3eh.isRetryableS3Error(err, errorType)
}

// GetTroubleshootingMessage provides actionable troubleshooting guidance for errors
func (s3eh *S3ErrorHandlerImpl) GetTroubleshootingMessage(err error, errorType ErrorType) string {
	if err == nil {
		return ""
	}

	errMsg := strings.ToLower(err.Error())
	
	switch errorType {
	case ErrorTypeS3Access:
		if strings.Contains(errMsg, "access denied") || strings.Contains(errMsg, "forbidden") {
			return "S3 Access Denied: Check your AWS credentials and IAM permissions. Ensure the IAM user/role has s3:GetObject and s3:ListBucket permissions for the specified bucket."
		}
		if strings.Contains(errMsg, "no such bucket") || strings.Contains(errMsg, "nosuchbucket") {
			return "S3 Bucket Not Found: Verify the bucket name is correct and exists in the specified region. Check if the bucket name contains typos."
		}
		if strings.Contains(errMsg, "invalid access key") {
			return "Invalid AWS Credentials: Check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, or ensure your IAM role is properly configured."
		}
		if strings.Contains(errMsg, "region") {
			return "S3 Region Mismatch: Ensure the specified region matches the bucket's actual region. Use 'aws s3api get-bucket-location --bucket <bucket-name>' to verify."
		}
		return "S3 Access Error: Verify your AWS credentials, bucket name, and region configuration. Check IAM permissions for S3 access."
		
	case ErrorTypeS3Download:
		if strings.Contains(errMsg, "timeout") {
			return "Download Timeout: The file download timed out. This may be due to large file size or slow network. Consider increasing timeout values or checking network connectivity."
		}
		if strings.Contains(errMsg, "connection") {
			return "Connection Error: Network connection to S3 failed. Check your internet connectivity and firewall settings. Retries will be attempted automatically."
		}
		if strings.Contains(errMsg, "slowdown") || strings.Contains(errMsg, "rate") {
			return "S3 Rate Limiting: S3 is throttling requests. The system will automatically back off and retry. Consider reducing concurrent download workers."
		}
		if strings.Contains(errMsg, "no such key") {
			return "S3 Object Not Found: The specified file no longer exists in the bucket. It may have been deleted or moved."
		}
		return "Download Error: File download from S3 failed. Check network connectivity and S3 service status. Retries will be attempted automatically."
		
	case ErrorTypeS3State:
		if strings.Contains(errMsg, "permission") {
			return "State File Permission Error: Cannot read/write state file. Check file permissions and ensure the directory is writable."
		}
		if strings.Contains(errMsg, "corruption") {
			return "State File Corruption: The state file is corrupted. Consider deleting it to rebuild state, but this will cause reprocessing of all files."
		}
		if strings.Contains(errMsg, "lock") {
			return "State File Lock Error: Another process may be using the state file. Ensure only one instance is running or check for stale lock files."
		}
		return "State Management Error: Error managing processing state. Check file permissions and disk space."
		
	case ErrorTypeNetwork:
		if strings.Contains(errMsg, "dns") {
			return "DNS Resolution Error: Cannot resolve S3 endpoint. Check your DNS configuration and internet connectivity."
		}
		if strings.Contains(errMsg, "timeout") {
			return "Network Timeout: Request timed out. Check network connectivity and consider increasing timeout values."
		}
		return "Network Error: Network connectivity issue. Check internet connection and firewall settings."
		
	case ErrorTypeMemory:
		return "Memory Error: Insufficient memory for operation. Consider reducing batch size, concurrent workers, or increasing available memory."
		
	case ErrorTypeFile:
		return "File System Error: Local file system error. Check disk space, permissions, and file system health."
		
	default:
		return fmt.Sprintf("Error: %s. Check logs for more details and consider retrying the operation.", err.Error())
	}
}

// classifyS3Error classifies S3-specific errors
func (s3eh *S3ErrorHandlerImpl) classifyS3Error(err error, operation string) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}

	errMsg := strings.ToLower(err.Error())
	opLower := strings.ToLower(operation)
	
	// S3 access errors
	if strings.Contains(errMsg, "access denied") ||
		strings.Contains(errMsg, "forbidden") ||
		strings.Contains(errMsg, "invalid access key") ||
		strings.Contains(errMsg, "no such bucket") ||
		strings.Contains(errMsg, "bucket not found") ||
		strings.Contains(opLower, "list") ||
		strings.Contains(opLower, "access") {
		return ErrorTypeS3Access
	}
	
	// S3 download errors
	if strings.Contains(errMsg, "no such key") ||
		strings.Contains(errMsg, "download") ||
		strings.Contains(opLower, "download") ||
		strings.Contains(opLower, "get") {
		return ErrorTypeS3Download
	}
	
	// State management errors
	if strings.Contains(opLower, "state") ||
		strings.Contains(errMsg, "state") ||
		strings.Contains(errMsg, "lock") ||
		strings.Contains(errMsg, "corruption") {
		return ErrorTypeS3State
	}
	
	// Fall back to general classification
	return s3eh.classifier.ClassifyError(err, operation)
}

// isRetryableS3Error determines if an S3 error is retryable
func (s3eh *S3ErrorHandlerImpl) isRetryableS3Error(err error, errorType ErrorType) bool {
	if err == nil {
		return false
	}

	config, exists := s3eh.retryConfigs[errorType]
	if !exists {
		return false
	}
	
	errMsg := strings.ToLower(err.Error())
	
	// Check against retryable error patterns
	for _, pattern := range config.RetryableErrors {
		if strings.Contains(errMsg, strings.ToLower(pattern)) {
			return true
		}
	}
	
	// Additional S3-specific retryable errors
	retryablePatterns := []string{
		"requesttimeout",
		"serviceunavailable", 
		"slowdown",
		"internalerror",
		"connection reset",
		"connection refused",
		"timeout",
		"temporary failure",
		"try again",
	}
	
	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	
	return false
}

// isRateLimitError checks if the error is due to rate limiting
func (s3eh *S3ErrorHandlerImpl) isRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "slowdown") ||
		strings.Contains(errMsg, "rate") ||
		strings.Contains(errMsg, "throttl") ||
		strings.Contains(errMsg, "too many requests")
}

// isAuthenticationError checks if the error is due to authentication issues
func (s3eh *S3ErrorHandlerImpl) isAuthenticationError(err error) bool {
	if err == nil {
		return false
	}
	
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "access denied") ||
		strings.Contains(errMsg, "forbidden") ||
		strings.Contains(errMsg, "invalid access key") ||
		strings.Contains(errMsg, "invalidaccesskeyid") ||
		strings.Contains(errMsg, "signature") ||
		strings.Contains(errMsg, "credential")
}