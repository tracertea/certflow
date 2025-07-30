package ctprocessor

import (
	"compress/gzip"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewStreamingProcessor(t *testing.T) {
	// Test with nil logger
	sp1 := NewStreamingProcessor(nil)
	if sp1 == nil {
		t.Fatal("NewStreamingProcessor() returned nil")
	}
	if sp1.logger == nil {
		t.Error("NewStreamingProcessor() should set default logger when nil provided")
	}

	// Test with custom logger
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	sp2 := NewStreamingProcessor(logger)
	if sp2 == nil {
		t.Fatal("NewStreamingProcessor() returned nil")
	}
	if sp2.logger != logger {
		t.Error("NewStreamingProcessor() should use provided logger")
	}
}

func TestStreamingProcessor_ProcessFile(t *testing.T) {
	sp := NewStreamingProcessor(nil)
	tempDir := t.TempDir()

	// Create a test gzip file with certificate data
	testGzipFile := createTestGzipFile(t, tempDir, "test.gz", []string{
		"0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MDkxNDAzNDVaFw0xNzA4MjUxOTE3NTlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIyODc0MjUwNzczMDkwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCOhY+wZx0Ek4w1BUaRtFJjmc8ZfkeiwSlCYue6EDiTyTZrpvkTzoBac9qgakH4Eh47Vr5fwGJicH2mKJ06nmzXkKwhvShxKN/eC95sEoYmUtiWPrW3IaDugNfLyzFQn19L9d8n71QFaEK7cUBE2+xNvJ5OkHOvD7hiuhvk2IRSwB12q0vp2eDhbEyf574XHnqk6R+ncauIQlRvv8rJ8MXRhzDCQScZolUL24xbpAv2UqxU0dfL8ZT0Ux0ebCL7Y+boVF5CbYaIiW0/qnW64yc87AhBsOTgqx1Qcq6qUhRm0tD8k4MmdCUhfo3YF9B34HOX0HsQhCaiMUtsRFQ0OdOxAgMBAAGjgaAwgZ0wEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFM4DfrlPGK8DoEi5z/ruSOyzqt+iMBMGCisGAQQB1nkCBAMBAf8EAgUAMA0GCSqGSIb3DQEBCwUAA4ICAQBOilO5T+iijwaJu4wk9nypEAiPc1i+fsT+T8oVhgKSi7vaOQs4LqA2Ay+2YVqfn5fwxuLdDGlNBtJUVa9tsZWIlQn7z4r7gSvCgo1Uvyu+gGEFsZ9uTrsHf6x0UuPW+ExPKgcsOVhno/UDi9zWz56JS9HF/mYwMpIwlqFaJOBlN1eMMoVx8rt8LAivHeaJScMyaQurZ3qWdEN5mSl7EmPuf6YYjZ8heZcn861BQLVc0SOW/ahWll16z2yYCoB93q2eGYDMD0Aw2kX1XFPemPAR5zsxzQyzMyqDH3NPc+lC2z7JKftkbJ9QzTxO2lzSYdnTt1ZJEimXerJe3tuabUaWcxNHPPOwGYVlL1yi+miYF97lbxgphNpkPHcrVp3k/p08feGbDaIQl8bD82Xme3wScTKT5J4PLlNiEPuOH8vIweOTkwGQjyLwE+4FX9RQ9ftCvIs8HqD0QxrQlKcRhcTLpfl1sXeWnHMzJWlGnh7qGrD82RvuXOxh2egIQ6BMFKr6s9lau1kunxU4YoyAQtgzWPk0hDfOUbk/WCX3d0gPQoaldRBSsFR7mwtI//M71+lAHI0hSBzxkgdaDyhO2a2upY5f50rCpT3Ef/UOpiq5hEYbw80ukn0oLxY53gU6BDz0Hh29pNJzCdJp4bbVYbRscxqKf7t7WfPYaVnnUaHVzg==",
	})

	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid JSON processing",
			config: Config{
				InputPath:    testGzipFile,
				OutputPath:   filepath.Join(tempDir, "output.json"),
				OutputFormat: "json",
				WorkerCount:  1,
				BatchSize:    100,
				LogLevel:     "info",
			},
			wantErr: false,
		},
		{
			name: "valid CSV processing",
			config: Config{
				InputPath:    testGzipFile,
				OutputPath:   filepath.Join(tempDir, "output.csv"),
				OutputFormat: "csv",
				WorkerCount:  1,
				BatchSize:    100,
				LogLevel:     "info",
			},
			wantErr: false,
		},
		{
			name: "valid text processing",
			config: Config{
				InputPath:    testGzipFile,
				OutputPath:   filepath.Join(tempDir, "output.txt"),
				OutputFormat: "txt",
				WorkerCount:  1,
				BatchSize:    100,
				LogLevel:     "info",
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: Config{
				InputPath:    "",
				OutputPath:   filepath.Join(tempDir, "output.json"),
				OutputFormat: "json",
			},
			wantErr: true,
			errMsg:  "invalid configuration",
		},
		{
			name: "non-existent input file",
			config: Config{
				InputPath:    "/path/to/nonexistent.gz",
				OutputPath:   filepath.Join(tempDir, "output.json"),
				OutputFormat: "json",
				WorkerCount:  1,
				BatchSize:    100,
			},
			wantErr: true,
			errMsg:  "failed to process file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sp.ProcessFile(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ProcessFile() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ProcessFile() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ProcessFile() unexpected error = %v", err)
					return
				}

				// Verify output file was created
				if _, err := os.Stat(tt.config.OutputPath); os.IsNotExist(err) {
					t.Errorf("ProcessFile() did not create output file: %s", tt.config.OutputPath)
				}
			}
		})
	}
}

func TestStreamingProcessor_ProcessBatch(t *testing.T) {
	sp := NewStreamingProcessor(nil)
	tempDir := t.TempDir()

	// Create multiple test gzip files
	testFile1 := createTestGzipFile(t, tempDir, "test1.gz", []string{
		"0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MDkxNDAzNDVaFw0xNzA4MjUxOTE3NTlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIyODc0MjUwNzczMDkwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCOhY+wZx0Ek4w1BUaRtFJjmc8ZfkeiwSlCYue6EDiTyTZrpvkTzoBac9qgakH4Eh47Vr5fwGJicH2mKJ06nmzXkKwhvShxKN/eC95sEoYmUtiWPrW3IaDugNfLyzFQn19L9d8n71QFaEK7cUBE2+xNvJ5OkHOvD7hiuhvk2IRSwB12q0vp2eDhbEyf574XHnqk6R+ncauIQlRvv8rJ8MXRhzDCQScZolUL24xbpAv2UqxU0dfL8ZT0Ux0ebCL7Y+boVF5CbYaIiW0/qnW64yc87AhBsOTgqx1Qcq6qUhRm0tD8k4MmdCUhfo3YF9B34HOX0HsQhCaiMUtsRFQ0OdOxAgMBAAGjgaAwgZ0wEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFM4DfrlPGK8DoEi5z/ruSOyzqt+iMBMGCisGAQQB1nkCBAMBAf8EAgUAMA0GCSqGSIb3DQEBCwUAA4ICAQBOilO5T+iijwaJu4wk9nypEAiPc1i+fsT+T8oVhgKSi7vaOQs4LqA2Ay+2YVqfn5fwxuLdDGlNBtJUVa9tsZWIlQn7z4r7gSvCgo1Uvyu+gGEFsZ9uTrsHf6x0UuPW+ExPKgcsOVhno/UDi9zWz56JS9HF/mYwMpIwlqFaJOBlN1eMMoVx8rt8LAivHeaJScMyaQurZ3qWdEN5mSl7EmPuf6YYjZ8heZcn861BQLVc0SOW/ahWll16z2yYCoB93q2eGYDMD0Aw2kX1XFPemPAR5zsxzQyzMyqDH3NPc+lC2z7JKftkbJ9QzTxO2lzSYdnTt1ZJEimXerJe3tuabUaWcxNHPPOwGYVlL1yi+miYF97lbxgphNpkPHcrVp3k/p08feGbDaIQl8bD82Xme3wScTKT5J4PLlNiEPuOH8vIweOTkwGQjyLwE+4FX9RQ9ftCvIs8HqD0QxrQlKcRhcTLpfl1sXeWnHMzJWlGnh7qGrD82RvuXOxh2egIQ6BMFKr6s9lau1kunxU4YoyAQtgzWPk0hDfOUbk/WCX3d0gPQoaldRBSsFR7mwtI//M71+lAHI0hSBzxkgdaDyhO2a2upY5f50rCpT3Ef/UOpiq5hEYbw80ukn0oLxY53gU6BDz0Hh29pNJzCdJp4bbVYbRscxqKf7t7WfPYaVnnUaHVzg==",
	})

	testFile2 := createTestGzipFile(t, tempDir, "test2.gz", []string{
		"1 MIIE7zCCAtegAwIBAgIHBVZlioDhNTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MTAxMjQzMThaFw0xNzA4MzAwOTQ2MjlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIzNjg5OTg5NDA5ODEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC8IayGYkGmso9thX5tfE/cl1Fic82bbKTU8Vapo1A4m1YPgJnd7rVFKIgipIfJadJcUIQUjQanZXBhv1rWi/47hl3GixYAskTFogcOVbmTL2uIo7q3inQDaU1bQrB/tf4uX4VSCvnfOx7604p0HzSLRbGnJfG+pQ5Rsu6nm4SniPDzLk6D5QP3GCG15q7zdrIMswkEXAsdo3nR4NbbqeEM9FSWztI74RRDb8DoYcbdAb2O3EC9CT7rmvRCHrn3ZwF8uFv36DH3goKSkeTxWmdHvuUyIXXXfcdQK7q/CokAedSIhd9bKhz73HnFooxI7plYhqSLi+xK1tL3TGpJt7QtAgMBAAGjgYswgYgwEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFD2IAzoQUeH3raVegkIGSAdwmWMgMA0GCSqGSIb3DQEBCwUAA4ICAQC0XG83k2PJvEMXY5p47n/Unn8vbMzl8QxkIR4+mD06N46BWR5BkGms824h8XV8e9G3Ku5XhlJQQXnUGSBItrAKEAQZiI5O+5/w21wz6auPXVhesZs/ekatdYbJrTnSdk88nqs/rA9gAB7RBh/z9ydCmMGyYMvhN3e2ama/G40VicZ/wutB+bZY7Jqeh6Yp/ZgfyCCP4bR5UArqgHi4fGPNbh4KDNnxJ0SKfiCYfsiAZRcKl0rWun5HDjLD+IOZVrf4toHG7ssm/ljclOueaW+nHZA5dWgMnzAHhxCZxXoN3olZhIif1A8HSgmdi/yB0ReTHD2LRe7i0QQCJ7F+1w+hR0zeaHECBTZgid1Z4lp7/6yQHVQ6UNlJzDs+H1Sll/0H1Cd3qQiI+dLbfJkG+7D846eHM+RwVg71LGmwFwY5ETcA/Npt1sq0GtpTO3yYiZtkwLIy6psL/R7y54QQKD3BhoaPMzN8hgTKERrLfDcV+c5Hl7ngMotn/6VLaPJLljXVSMzHsJ2Iezua3vYB2FcyiEna7/UvceZlEViVEZzW4Smh4tEjHcx2gLy6shxEUjy8y49Mwfy94nFQpLLVwx1+AspL3xAQ9TGDt1arCnTj6bULuxmy+iE4lPjqjHe+Iv3WATHc6thpdQGJmCN06RATzJAPW8JaNVE5QDvGz3Jcvw==",
	})

	config := Config{
		OutputPath:   filepath.Join(tempDir, "batch_output.json"),
		OutputFormat: "json",
		WorkerCount:  1,
		BatchSize:    100,
		LogLevel:     "info",
	}

	tests := []struct {
		name        string
		inputPaths  []string
		config      Config
		wantErr     bool
		errMsg      string
	}{
		{
			name:       "valid batch processing",
			inputPaths: []string{testFile1, testFile2},
			config:     config,
			wantErr:    false,
		},
		{
			name:       "empty input paths",
			inputPaths: []string{},
			config:     config,
			wantErr:    true,
			errMsg:     "no input files provided",
		},
		{
			name:       "invalid config",
			inputPaths: []string{testFile1},
			config: Config{
				OutputPath:   "",
				OutputFormat: "json",
			},
			wantErr: true,
			errMsg:  "invalid configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sp.ProcessBatch(tt.inputPaths, tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ProcessBatch() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ProcessBatch() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ProcessBatch() unexpected error = %v", err)
					return
				}

				// Verify output file was created
				if _, err := os.Stat(tt.config.OutputPath); os.IsNotExist(err) {
					t.Errorf("ProcessBatch() did not create output file: %s", tt.config.OutputPath)
				}
			}
		})
	}
}

func TestStreamingProcessor_GetFileInfo(t *testing.T) {
	sp := NewStreamingProcessor(nil)
	tempDir := t.TempDir()

	// Create a test gzip file
	testFile := createTestGzipFile(t, tempDir, "info_test.gz", []string{
		"0 test_cert_1",
		"1 test_cert_2",
		"2 test_cert_3",
	})

	tests := []struct {
		name     string
		filePath string
		wantErr  bool
		errMsg   string
		wantCount int64
	}{
		{
			name:      "valid file",
			filePath:  testFile,
			wantErr:   false,
			wantCount: 3,
		},
		{
			name:     "non-existent file",
			filePath: "/path/to/nonexistent.gz",
			wantErr:  true,
			errMsg:   "file does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := sp.GetFileInfo(tt.filePath)
			if tt.wantErr {
				if err == nil {
					t.Errorf("GetFileInfo() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("GetFileInfo() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("GetFileInfo() unexpected error = %v", err)
					return
				}
				if info == nil {
					t.Error("GetFileInfo() returned nil info")
					return
				}
				if info.LineCount != tt.wantCount {
					t.Errorf("GetFileInfo() line count = %d, want %d", info.LineCount, tt.wantCount)
				}
				if info.Path != tt.filePath {
					t.Errorf("GetFileInfo() path = %s, want %s", info.Path, tt.filePath)
				}
			}
		})
	}
}

func TestProgressReporter(t *testing.T) {
	// Test with nil logger
	pr1 := NewProgressReporter(nil, 0)
	if pr1 == nil {
		t.Fatal("NewProgressReporter() returned nil")
	}
	if pr1.interval != 10000 {
		t.Errorf("NewProgressReporter() interval = %d, want 10000", pr1.interval)
	}

	// Test with custom values
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	pr2 := NewProgressReporter(logger, 5000)
	if pr2.interval != 5000 {
		t.Errorf("NewProgressReporter() interval = %d, want 5000", pr2.interval)
	}

	// Test reporting (just ensure no panics)
	stats := &ProcessingStats{
		TotalProcessed:   10000,
		SuccessfulParsed: 9500,
		Errors:          500,
	}

	pr2.Report(stats)
	pr2.ReportFinal(stats)
}

// Helper function to create test gzip files
func createTestGzipFile(t *testing.T, dir, filename string, lines []string) string {
	filePath := filepath.Join(dir, filename)
	
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	content := strings.Join(lines, "\n")
	if _, err := gzipWriter.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write gzip content: %v", err)
	}

	return filePath
}