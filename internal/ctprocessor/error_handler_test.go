package ctprocessor

import (
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewErrorHandler(t *testing.T) {
	stats := &ProcessingStats{}
	
	// Test with nil logger
	eh1 := NewErrorHandler(nil, stats, 100)
	if eh1 == nil {
		t.Fatal("NewErrorHandler() returned nil")
	}
	if eh1.logger == nil {
		t.Error("NewErrorHandler() should set default logger when nil provided")
	}
	if eh1.maxErrors != 100 {
		t.Errorf("NewErrorHandler() maxErrors = %d, want 100", eh1.maxErrors)
	}

	// Test with custom logger and zero maxErrors
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	eh2 := NewErrorHandler(logger, stats, 0)
	if eh2.maxErrors != 1000 {
		t.Errorf("NewErrorHandler() should set default maxErrors when <= 0, got %d", eh2.maxErrors)
	}
}

func TestErrorHandler_HandleError(t *testing.T) {
	stats := &ProcessingStats{}
	eh := NewErrorHandler(nil, stats, 5) // Max 5 errors

	tests := []struct {
		name         string
		err          error
		errorType    ErrorType
		context      string
		sequenceNum  int64
		wantContinue bool
		errorCount   int64
	}{
		{
			name:         "first error",
			err:          errors.New("test error 1"),
			errorType:    ErrorTypeParsing,
			context:      "parsing certificate",
			sequenceNum:  123,
			wantContinue: true,
			errorCount:   1,
		},
		{
			name:         "second error",
			err:          errors.New("test error 2"),
			errorType:    ErrorTypeFile,
			context:      "reading file",
			sequenceNum:  124,
			wantContinue: true,
			errorCount:   2,
		},
		{
			name:         "fifth error (at limit)",
			err:          errors.New("test error 5"),
			errorType:    ErrorTypeProcessing,
			context:      "processing certificate",
			sequenceNum:  127,
			wantContinue: false,
			errorCount:   5,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Add errors to reach the test case
			for j := int(eh.GetErrorCount()); j < int(tt.errorCount)-1; j++ {
				eh.HandleError(errors.New("filler error"), ErrorTypeUnknown, "test", int64(j))
			}

			shouldContinue := eh.HandleError(tt.err, tt.errorType, tt.context, tt.sequenceNum)
			
			if shouldContinue != tt.wantContinue {
				t.Errorf("HandleError() shouldContinue = %v, want %v", shouldContinue, tt.wantContinue)
			}

			if eh.GetErrorCount() != tt.errorCount {
				t.Errorf("HandleError() errorCount = %d, want %d", eh.GetErrorCount(), tt.errorCount)
			}

			if eh.GetErrorCountByType(tt.errorType) == 0 {
				t.Errorf("HandleError() should have recorded error of type %s", tt.errorType)
			}

			// Check that stats were updated
			if stats.Errors != tt.errorCount {
				t.Errorf("HandleError() stats.Errors = %d, want %d", stats.Errors, tt.errorCount)
			}
		})

		// Reset for next test if needed
		if i < len(tests)-1 {
			eh.Reset()
			stats.Errors = 0
		}
	}
}

func TestErrorHandler_GetErrorSummary(t *testing.T) {
	eh := NewErrorHandler(nil, nil, 100)

	// Add some errors
	eh.HandleError(errors.New("parsing error 1"), ErrorTypeParsing, "parse cert", 1)
	eh.HandleError(errors.New("parsing error 2"), ErrorTypeParsing, "parse cert", 2)
	eh.HandleError(errors.New("file error"), ErrorTypeFile, "read file", 3)

	summary := eh.GetErrorSummary()

	if summary.TotalErrors != 3 {
		t.Errorf("GetErrorSummary() TotalErrors = %d, want 3", summary.TotalErrors)
	}

	if summary.ErrorsByType[ErrorTypeParsing] != 2 {
		t.Errorf("GetErrorSummary() parsing errors = %d, want 2", summary.ErrorsByType[ErrorTypeParsing])
	}

	if summary.ErrorsByType[ErrorTypeFile] != 1 {
		t.Errorf("GetErrorSummary() file errors = %d, want 1", summary.ErrorsByType[ErrorTypeFile])
	}

	if len(summary.RecentErrors) != 3 {
		t.Errorf("GetErrorSummary() recent errors count = %d, want 3", len(summary.RecentErrors))
	}

	// Test string representation
	summaryStr := summary.String()
	if !strings.Contains(summaryStr, "Total errors: 3") {
		t.Errorf("ErrorSummary.String() should contain total errors, got: %s", summaryStr)
	}
}

func TestErrorHandler_Reset(t *testing.T) {
	eh := NewErrorHandler(nil, nil, 100)

	// Add some errors
	eh.HandleError(errors.New("error 1"), ErrorTypeParsing, "context", 1)
	eh.HandleError(errors.New("error 2"), ErrorTypeFile, "context", 2)

	// Verify errors were added
	if eh.GetErrorCount() != 2 {
		t.Errorf("Before reset: error count = %d, want 2", eh.GetErrorCount())
	}

	// Reset
	eh.Reset()

	// Verify reset worked
	if eh.GetErrorCount() != 0 {
		t.Errorf("After reset: error count = %d, want 0", eh.GetErrorCount())
	}

	summary := eh.GetErrorSummary()
	if summary.TotalErrors != 0 {
		t.Errorf("After reset: summary total errors = %d, want 0", summary.TotalErrors)
	}

	if len(summary.RecentErrors) != 0 {
		t.Errorf("After reset: recent errors count = %d, want 0", len(summary.RecentErrors))
	}
}

func TestProcessingError(t *testing.T) {
	cause := errors.New("underlying error")
	
	// Test with sequence number
	pe1 := ProcessingError{
		Type:        ErrorTypeParsing,
		Message:     "failed to parse certificate",
		Context:     "certificate parsing",
		SequenceNum: 123,
		Timestamp:   time.Now(),
		Cause:       cause,
	}

	errorStr := pe1.Error()
	if !strings.Contains(errorStr, "[PARSING]") {
		t.Errorf("ProcessingError.Error() should contain error type, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "seq:123") {
		t.Errorf("ProcessingError.Error() should contain sequence number, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "failed to parse certificate") {
		t.Errorf("ProcessingError.Error() should contain message, got: %s", errorStr)
	}

	// Test Unwrap
	if pe1.Unwrap() != cause {
		t.Errorf("ProcessingError.Unwrap() = %v, want %v", pe1.Unwrap(), cause)
	}

	// Test without sequence number
	pe2 := ProcessingError{
		Type:        ErrorTypeFile,
		Message:     "file not found",
		Context:     "file reading",
		SequenceNum: -1, // No sequence number
		Timestamp:   time.Now(),
	}

	errorStr2 := pe2.Error()
	if strings.Contains(errorStr2, "seq:") {
		t.Errorf("ProcessingError.Error() should not contain sequence number when < 0, got: %s", errorStr2)
	}
}

func TestErrorType_String(t *testing.T) {
	tests := []struct {
		errorType ErrorType
		want      string
	}{
		{ErrorTypeFile, "FILE"},
		{ErrorTypeParsing, "PARSING"},
		{ErrorTypeProcessing, "PROCESSING"},
		{ErrorTypeOutput, "OUTPUT"},
		{ErrorTypeNetwork, "NETWORK"},
		{ErrorTypeMemory, "MEMORY"},
		{ErrorTypeUnknown, "UNKNOWN"},
		{ErrorType(999), "UNKNOWN"}, // Invalid type
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.errorType.String(); got != tt.want {
				t.Errorf("ErrorType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorClassifier_ClassifyError(t *testing.T) {
	ec := NewErrorClassifier()

	tests := []struct {
		name     string
		err      error
		context  string
		want     ErrorType
	}{
		{
			name:    "file not found error",
			err:     errors.New("no such file or directory"),
			context: "reading input file",
			want:    ErrorTypeFile,
		},
		{
			name:    "permission denied error",
			err:     errors.New("permission denied"),
			context: "opening file",
			want:    ErrorTypeFile,
		},
		{
			name:    "parsing error",
			err:     errors.New("failed to parse certificate"),
			context: "certificate parsing",
			want:    ErrorTypeParsing,
		},
		{
			name:    "base64 decode error",
			err:     errors.New("invalid base64 encoding"),
			context: "decoding certificate data",
			want:    ErrorTypeParsing,
		},
		{
			name:    "memory error",
			err:     errors.New("out of memory"),
			context: "processing large file",
			want:    ErrorTypeMemory,
		},
		{
			name:    "network error",
			err:     errors.New("connection timeout"),
			context: "downloading file",
			want:    ErrorTypeNetwork,
		},
		{
			name:    "output error",
			err:     errors.New("failed to write output"),
			context: "writing results",
			want:    ErrorTypeOutput,
		},
		{
			name:    "processing error",
			err:     errors.New("failed to extract mappings"),
			context: "processing certificate",
			want:    ErrorTypeProcessing,
		},
		{
			name:    "unknown error",
			err:     errors.New("something went wrong"),
			context: "unknown operation",
			want:    ErrorTypeUnknown,
		},
		{
			name:    "nil error",
			err:     nil,
			context: "test",
			want:    ErrorTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ec.ClassifyError(tt.err, tt.context)
			if got != tt.want {
				t.Errorf("ClassifyError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecoveryStrategy(t *testing.T) {
	rs := NewRecoveryStrategy()

	tests := []struct {
		name         string
		err          error
		context      string
		attemptCount int
		wantRetry    bool
		wantAction   RecoveryAction
	}{
		{
			name:         "network error - should retry",
			err:          errors.New("connection timeout"),
			context:      "network operation",
			attemptCount: 1,
			wantRetry:    true,
			wantAction:   RecoveryActionRetryWithDelay,
		},
		{
			name:         "network error - max retries reached",
			err:          errors.New("connection timeout"),
			context:      "network operation",
			attemptCount: 3,
			wantRetry:    false,
			wantAction:   RecoveryActionRetryWithDelay,
		},
		{
			name:         "parsing error - should not retry",
			err:          errors.New("invalid certificate format"),
			context:      "parsing certificate",
			attemptCount: 0,
			wantRetry:    false,
			wantAction:   RecoveryActionSkip,
		},
		{
			name:         "memory error - should reduce load",
			err:          errors.New("out of memory"),
			context:      "processing large dataset",
			attemptCount: 0,
			wantRetry:    true,
			wantAction:   RecoveryActionReduceLoad,
		},
		{
			name:         "file error - should retry once",
			err:          errors.New("file temporarily unavailable"),
			context:      "reading file",
			attemptCount: 1,
			wantRetry:    true,
			wantAction:   RecoveryActionRetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldRetry := rs.ShouldRetry(tt.err, tt.context, tt.attemptCount)
			if shouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry() = %v, want %v", shouldRetry, tt.wantRetry)
			}

			action := rs.GetRecoveryAction(tt.err, tt.context)
			if action != tt.wantAction {
				t.Errorf("GetRecoveryAction() = %v, want %v", action, tt.wantAction)
			}
		})
	}
}

func TestRecoveryAction_String(t *testing.T) {
	tests := []struct {
		action RecoveryAction
		want   string
	}{
		{RecoveryActionSkip, "SKIP"},
		{RecoveryActionRetry, "RETRY"},
		{RecoveryActionRetryWithDelay, "RETRY_WITH_DELAY"},
		{RecoveryActionReduceLoad, "REDUCE_LOAD"},
		{RecoveryActionStop, "STOP"},
		{RecoveryAction(999), "UNKNOWN"}, // Invalid action
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.action.String(); got != tt.want {
				t.Errorf("RecoveryAction.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorSummary_String_NoErrors(t *testing.T) {
	summary := ErrorSummary{
		TotalErrors:  0,
		ErrorsByType: make(map[ErrorType]int64),
		RecentErrors: make([]ProcessingError, 0),
	}

	result := summary.String()
	expected := "No errors encountered"
	
	if result != expected {
		t.Errorf("ErrorSummary.String() for no errors = %v, want %v", result, expected)
	}
}

// S3 Error Handler Tests

func TestNewS3ErrorHandler(t *testing.T) {
	stats := &ProcessingStats{}
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	
	s3eh := NewS3ErrorHandler(logger, stats, 100)
	
	if s3eh == nil {
		t.Fatal("NewS3ErrorHandler() returned nil")
	}
	
	if s3eh.ErrorHandler == nil {
		t.Error("NewS3ErrorHandler() should embed ErrorHandler")
	}
	
	if len(s3eh.retryConfigs) == 0 {
		t.Error("NewS3ErrorHandler() should initialize retry configs")
	}
	
	// Check that S3-specific retry configs are set
	expectedTypes := []ErrorType{ErrorTypeS3Access, ErrorTypeS3Download, ErrorTypeS3State, ErrorTypeNetwork}
	for _, errorType := range expectedTypes {
		if _, exists := s3eh.retryConfigs[errorType]; !exists {
			t.Errorf("NewS3ErrorHandler() missing retry config for %s", errorType)
		}
	}
}

func TestS3ErrorHandler_HandleS3Error(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name      string
		err       error
		operation string
		want      ErrorAction
	}{
		{
			name:      "nil error",
			err:       nil,
			operation: "list objects",
			want:      ErrorActionRetry,
		},
		{
			name:      "access denied error",
			err:       errors.New("AccessDenied: Access Denied"),
			operation: "list objects",
			want:      ErrorActionFail,
		},
		{
			name:      "rate limit error",
			err:       errors.New("SlowDown: Please reduce your request rate"),
			operation: "list objects",
			want:      ErrorActionWait,
		},
		{
			name:      "retryable timeout error",
			err:       errors.New("RequestTimeout: Your socket connection to the server was not read from or written to within the timeout period"),
			operation: "list objects",
			want:      ErrorActionRetry,
		},
		{
			name:      "no such bucket error",
			err:       errors.New("NoSuchBucket: The specified bucket does not exist"),
			operation: "list objects",
			want:      ErrorActionFail,
		},
		{
			name:      "service unavailable error",
			err:       errors.New("ServiceUnavailable: Please try again"),
			operation: "get object",
			want:      ErrorActionRetry,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.HandleS3Error(tt.err, tt.operation)
			if got != tt.want {
				t.Errorf("HandleS3Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_HandleDownloadError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	object := S3Object{
		Key:  "test/file.gz",
		Size: 1024,
		ETag: "abc123",
	}
	
	tests := []struct {
		name    string
		err     error
		object  S3Object
		attempt int
		want    ErrorAction
	}{
		{
			name:    "nil error",
			err:     nil,
			object:  object,
			attempt: 1,
			want:    ErrorActionRetry,
		},
		{
			name:    "first timeout attempt",
			err:     errors.New("connection timeout"),
			object:  object,
			attempt: 1,
			want:    ErrorActionRetry,
		},
		{
			name:    "max retries reached",
			err:     errors.New("connection timeout"),
			object:  object,
			attempt: 10,
			want:    ErrorActionFail,
		},
		{
			name:    "rate limit error",
			err:     errors.New("SlowDown: Please reduce your request rate"),
			object:  object,
			attempt: 3,
			want:    ErrorActionWait,
		},
		{
			name:    "non-retryable error",
			err:     errors.New("invalid file format"),
			object:  object,
			attempt: 1,
			want:    ErrorActionSkip,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.HandleDownloadError(tt.err, tt.object, tt.attempt)
			if got != tt.want {
				t.Errorf("HandleDownloadError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_HandleProcessingError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name     string
		err      error
		filePath string
		want     ErrorAction
	}{
		{
			name:     "nil error",
			err:      nil,
			filePath: "/tmp/test.gz",
			want:     ErrorActionRetry,
		},
		{
			name:     "memory error",
			err:      errors.New("out of memory"),
			filePath: "/tmp/test.gz",
			want:     ErrorActionWait,
		},
		{
			name:     "file lock error",
			err:      errors.New("file is locked by another process"),
			filePath: "/tmp/test.gz",
			want:     ErrorActionRetry,
		},
		{
			name:     "parsing error",
			err:      errors.New("invalid certificate format"),
			filePath: "/tmp/test.gz",
			want:     ErrorActionSkip,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.HandleProcessingError(tt.err, tt.filePath)
			if got != tt.want {
				t.Errorf("HandleProcessingError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_HandleStateError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name string
		err  error
		want ErrorAction
	}{
		{
			name: "nil error",
			err:  nil,
			want: ErrorActionRetry,
		},
		{
			name: "permission error",
			err:  errors.New("permission denied"),
			want: ErrorActionRetry,
		},
		{
			name: "corruption error",
			err:  errors.New("state file corruption detected"),
			want: ErrorActionFail,
		},
		{
			name: "lock error",
			err:  errors.New("file is busy"),
			want: ErrorActionRetry,
		},
		{
			name: "unknown state error",
			err:  errors.New("unknown state error"),
			want: ErrorActionSkip,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.HandleStateError(tt.err)
			if got != tt.want {
				t.Errorf("HandleStateError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_GetRetryDelay(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name      string
		attempt   int
		errorType ErrorType
		wantMin   time.Duration
		wantMax   time.Duration
	}{
		{
			name:      "first S3 access retry",
			attempt:   0,
			errorType: ErrorTypeS3Access,
			wantMin:   900 * time.Millisecond,  // 1s - 10% jitter
			wantMax:   1100 * time.Millisecond, // 1s + 10% jitter
		},
		{
			name:      "second S3 access retry",
			attempt:   1,
			errorType: ErrorTypeS3Access,
			wantMin:   1800 * time.Millisecond, // 2s - 10% jitter
			wantMax:   2200 * time.Millisecond, // 2s + 10% jitter
		},
		{
			name:      "max delay reached",
			attempt:   10,
			errorType: ErrorTypeS3Access,
			wantMin:   27 * time.Second,  // 30s - 10% jitter
			wantMax:   33 * time.Second,  // 30s + 10% jitter
		},
		{
			name:      "unknown error type",
			attempt:   1,
			errorType: ErrorTypeUnknown,
			wantMin:   900 * time.Millisecond,  // Default config
			wantMax:   2200 * time.Millisecond, // Default config with backoff
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := s3eh.GetRetryDelay(tt.attempt, tt.errorType)
			if delay < tt.wantMin || delay > tt.wantMax {
				t.Errorf("GetRetryDelay() = %v, want between %v and %v", delay, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestS3ErrorHandler_IsRetryableError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name      string
		err       error
		errorType ErrorType
		want      bool
	}{
		{
			name:      "nil error",
			err:       nil,
			errorType: ErrorTypeS3Access,
			want:      false,
		},
		{
			name:      "S3 timeout error",
			err:       errors.New("RequestTimeout: timeout occurred"),
			errorType: ErrorTypeS3Access,
			want:      true,
		},
		{
			name:      "S3 service unavailable",
			err:       errors.New("ServiceUnavailable: service is unavailable"),
			errorType: ErrorTypeS3Access,
			want:      true,
		},
		{
			name:      "S3 slowdown",
			err:       errors.New("SlowDown: please reduce request rate"),
			errorType: ErrorTypeS3Download,
			want:      true,
		},
		{
			name:      "connection error",
			err:       errors.New("connection reset by peer"),
			errorType: ErrorTypeS3Download,
			want:      true,
		},
		{
			name:      "access denied - not retryable",
			err:       errors.New("AccessDenied: access denied"),
			errorType: ErrorTypeS3Access,
			want:      false,
		},
		{
			name:      "invalid format - not retryable",
			err:       errors.New("invalid file format"),
			errorType: ErrorTypeS3Download,
			want:      false,
		},
		{
			name:      "unknown error type",
			err:       errors.New("timeout"),
			errorType: ErrorTypeUnknown,
			want:      false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.IsRetryableError(tt.err, tt.errorType)
			if got != tt.want {
				t.Errorf("IsRetryableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_GetTroubleshootingMessage(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name      string
		err       error
		errorType ErrorType
		wantContains []string
	}{
		{
			name:      "nil error",
			err:       nil,
			errorType: ErrorTypeS3Access,
			wantContains: []string{},
		},
		{
			name:      "S3 access denied",
			err:       errors.New("AccessDenied: Access Denied"),
			errorType: ErrorTypeS3Access,
			wantContains: []string{"AWS credentials", "IAM permissions", "s3:GetObject"},
		},
		{
			name:      "S3 bucket not found",
			err:       errors.New("NoSuchBucket: The specified bucket does not exist"),
			errorType: ErrorTypeS3Access,
			wantContains: []string{"bucket name", "correct", "region"},
		},
		{
			name:      "download timeout",
			err:       errors.New("connection timeout"),
			errorType: ErrorTypeS3Download,
			wantContains: []string{"timeout", "network", "large file"},
		},
		{
			name:      "rate limiting",
			err:       errors.New("SlowDown: Please reduce your request rate"),
			errorType: ErrorTypeS3Download,
			wantContains: []string{"throttling", "back off", "concurrent"},
		},
		{
			name:      "state corruption",
			err:       errors.New("state file corruption detected"),
			errorType: ErrorTypeS3State,
			wantContains: []string{"corrupted", "deleting", "rebuild"},
		},
		{
			name:      "memory error",
			err:       errors.New("out of memory"),
			errorType: ErrorTypeMemory,
			wantContains: []string{"memory", "batch size", "workers"},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.GetTroubleshootingMessage(tt.err, tt.errorType)
			
			if tt.err == nil {
				if got != "" {
					t.Errorf("GetTroubleshootingMessage() for nil error = %v, want empty string", got)
				}
				return
			}
			
			for _, want := range tt.wantContains {
				if !strings.Contains(strings.ToLower(got), strings.ToLower(want)) {
					t.Errorf("GetTroubleshootingMessage() = %v, should contain %v", got, want)
				}
			}
		})
	}
}

func TestErrorClassifier_ClassifyS3Error(t *testing.T) {
	ec := NewErrorClassifier()
	
	tests := []struct {
		name    string
		err     error
		context string
		want    ErrorType
	}{
		{
			name:    "S3 access denied",
			err:     errors.New("AccessDenied: Access Denied"),
			context: "s3 list objects",
			want:    ErrorTypeS3Access,
		},
		{
			name:    "S3 bucket not found",
			err:     errors.New("NoSuchBucket: The specified bucket does not exist"),
			context: "s3 access",
			want:    ErrorTypeS3Access,
		},
		{
			name:    "S3 object not found",
			err:     errors.New("NoSuchKey: The specified key does not exist"),
			context: "s3 download",
			want:    ErrorTypeS3Download,
		},
		{
			name:    "S3 download error",
			err:     errors.New("connection timeout"),
			context: "s3 get object",
			want:    ErrorTypeS3Download,
		},
		{
			name:    "S3 state error",
			err:     errors.New("state file corruption"),
			context: "s3 state management",
			want:    ErrorTypeS3State,
		},
		{
			name:    "general S3 error",
			err:     errors.New("s3 service error"),
			context: "s3 operation",
			want:    ErrorTypeS3Access, // Falls back to access error for general S3 errors
		},
		{
			name:    "non-S3 error",
			err:     errors.New("file not found"),
			context: "local file operation",
			want:    ErrorTypeFile,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ec.ClassifyError(tt.err, tt.context)
			if got != tt.want {
				t.Errorf("ClassifyError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorAction_String(t *testing.T) {
	tests := []struct {
		action ErrorAction
		want   string
	}{
		{ErrorActionRetry, "RETRY"},
		{ErrorActionSkip, "SKIP"},
		{ErrorActionFail, "FAIL"},
		{ErrorActionWait, "WAIT"},
		{ErrorAction(999), "UNKNOWN"}, // Invalid action
	}
	
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.action.String(); got != tt.want {
				t.Errorf("ErrorAction.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorType_String(t *testing.T) {
	tests := []struct {
		errorType ErrorType
		want      string
	}{
		{ErrorTypeS3Access, "S3_ACCESS"},
		{ErrorTypeS3Download, "S3_DOWNLOAD"},
		{ErrorTypeS3State, "S3_STATE"},
	}
	
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.errorType.String(); got != tt.want {
				t.Errorf("ErrorType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecoveryStrategy_S3Errors(t *testing.T) {
	rs := NewRecoveryStrategy()
	
	tests := []struct {
		name         string
		err          error
		context      string
		attemptCount int
		wantRetry    bool
		wantAction   RecoveryAction
	}{
		{
			name:         "S3 access error - should retry with delay",
			err:          errors.New("ServiceUnavailable: service unavailable"),
			context:      "s3 list objects",
			attemptCount: 1,
			wantRetry:    true,
			wantAction:   RecoveryActionRetryWithDelay,
		},
		{
			name:         "S3 download error - should retry with delay",
			err:          errors.New("connection timeout"),
			context:      "s3 download file",
			attemptCount: 5,
			wantRetry:    true,
			wantAction:   RecoveryActionRetryWithDelay,
		},
		{
			name:         "S3 state error - should retry",
			err:          errors.New("file lock error"),
			context:      "s3 state update",
			attemptCount: 1,
			wantRetry:    true,
			wantAction:   RecoveryActionRetry,
		},
		{
			name:         "S3 access error - max retries reached",
			err:          errors.New("ServiceUnavailable: service unavailable"),
			context:      "s3 list objects",
			attemptCount: 3,
			wantRetry:    false,
			wantAction:   RecoveryActionRetryWithDelay,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldRetry := rs.ShouldRetry(tt.err, tt.context, tt.attemptCount)
			if shouldRetry != tt.wantRetry {
				t.Errorf("ShouldRetry() = %v, want %v", shouldRetry, tt.wantRetry)
			}
			
			action := rs.GetRecoveryAction(tt.err, tt.context)
			if action != tt.wantAction {
				t.Errorf("GetRecoveryAction() = %v, want %v", action, tt.wantAction)
			}
		})
	}
}

func TestS3ErrorHandler_isRateLimitError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "SlowDown error",
			err:  errors.New("SlowDown: Please reduce your request rate"),
			want: true,
		},
		{
			name: "rate limit error",
			err:  errors.New("rate limit exceeded"),
			want: true,
		},
		{
			name: "throttling error",
			err:  errors.New("request throttled"),
			want: true,
		},
		{
			name: "too many requests",
			err:  errors.New("too many requests"),
			want: true,
		},
		{
			name: "regular error",
			err:  errors.New("file not found"),
			want: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.isRateLimitError(tt.err)
			if got != tt.want {
				t.Errorf("isRateLimitError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3ErrorHandler_isAuthenticationError(t *testing.T) {
	s3eh := NewS3ErrorHandler(nil, &ProcessingStats{}, 100)
	
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "access denied",
			err:  errors.New("AccessDenied: Access Denied"),
			want: true,
		},
		{
			name: "forbidden",
			err:  errors.New("Forbidden: You don't have permission"),
			want: true,
		},
		{
			name: "invalid access key",
			err:  errors.New("InvalidAccessKeyId: The AWS Access Key Id you provided does not exist"),
			want: true,
		},
		{
			name: "signature error",
			err:  errors.New("SignatureDoesNotMatch: signature mismatch"),
			want: true,
		},
		{
			name: "credential error",
			err:  errors.New("credential error occurred"),
			want: true,
		},
		{
			name: "regular error",
			err:  errors.New("file not found"),
			want: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s3eh.isAuthenticationError(tt.err)
			if got != tt.want {
				t.Errorf("isAuthenticationError() = %v, want %v", got, tt.want)
			}
		})
	}
}