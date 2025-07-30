package ctprocessor

import (
	"compress/gzip"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewConcurrentProcessor(t *testing.T) {
	// Test with nil logger
	cp1 := NewConcurrentProcessor(nil)
	if cp1 == nil {
		t.Fatal("NewConcurrentProcessor() returned nil")
	}
	if cp1.logger == nil {
		t.Error("NewConcurrentProcessor() should set default logger when nil provided")
	}

	// Test with custom logger
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	cp2 := NewConcurrentProcessor(logger)
	if cp2 == nil {
		t.Fatal("NewConcurrentProcessor() returned nil")
	}
	if cp2.logger != logger {
		t.Error("NewConcurrentProcessor() should use provided logger")
	}
}

func TestConcurrentProcessor_ProcessFileWithWorkers(t *testing.T) {
	cp := NewConcurrentProcessor(nil)
	tempDir := t.TempDir()

	// Create a test gzip file with multiple certificate entries
	testGzipFile := createTestGzipFileForConcurrent(t, tempDir, "concurrent_test.gz", []string{
		"0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MDkxNDAzNDVaFw0xNzA4MjUxOTE3NTlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIyODc0MjUwNzczMDkwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCOhY+wZx0Ek4w1BUaRtFJjmc8ZfkeiwSlCYue6EDiTyTZrpvkTzoBac9qgakH4Eh47Vr5fwGJicH2mKJ06nmzXkKwhvShxKN/eC95sEoYmUtiWPrW3IaDugNfLyzFQn19L9d8n71QFaEK7cUBE2+xNvJ5OkHOvD7hiuhvk2IRSwB12q0vp2eDhbEyf574XHnqk6R+ncauIQlRvv8rJ8MXRhzDCQScZolUL24xbpAv2UqxU0dfL8ZT0Ux0ebCL7Y+boVF5CbYaIiW0/qnW64yc87AhBsOTgqx1Qcq6qUhRm0tD8k4MmdCUhfo3YF9B34HOX0HsQhCaiMUtsRFQ0OdOxAgMBAAGjgaAwgZ0wEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFM4DfrlPGK8DoEi5z/ruSOyzqt+iMBMGCisGAQQB1nkCBAMBAf8EAgUAMA0GCSqGSIb3DQEBCwUAA4ICAQBOilO5T+iijwaJu4wk9nypEAiPc1i+fsT+T8oVhgKSi7vaOQs4LqA2Ay+2YVqfn5fwxuLdDGlNBtJUVa9tsZWIlQn7z4r7gSvCgo1Uvyu+gGEFsZ9uTrsHf6x0UuPW+ExPKgcsOVhno/UDi9zWz56JS9HF/mYwMpIwlqFaJOBlN1eMMoVx8rt8LAivHeaJScMyaQurZ3qWdEN5mSl7EmPuf6YYjZ8heZcn861BQLVc0SOW/ahWll16z2yYCoB93q2eGYDMD0Aw2kX1XFPemPAR5zsxzQyzMyqDH3NPc+lC2z7JKftkbJ9QzTxO2lzSYdnTt1ZJEimXerJe3tuabUaWcxNHPPOwGYVlL1yi+miYF97lbxgphNpkPHcrVp3k/p08feGbDaIQl8bD82Xme3wScTKT5J4PLlNiEPuOH8vIweOTkwGQjyLwE+4FX9RQ9ftCvIs8HqD0QxrQlKcRhcTLpfl1sXeWnHMzJWlGnh7qGrD82RvuXOxh2egIQ6BMFKr6s9lau1kunxU4YoyAQtgzWPk0hDfOUbk/WCX3d0gPQoaldRBSsFR7mwtI//M71+lAHI0hSBzxkgdaDyhO2a2upY5f50rCpT3Ef/UOpiq5hEYbw80ukn0oLxY53gU6BDz0Hh29pNJzCdJp4bbVYbRscxqKf7t7WfPYaVnnUaHVzg==",
		"1 MIIE7zCCAtegAwIBAgIHBVZlioDhNTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MTAxMjQzMThaFw0xNzA4MzAwOTQ2MjlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIzNjg5OTg5NDA5ODEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC8IayGYkGmso9thX5tfE/cl1Fic82bbKTU8Vapo1A4m1YPgJnd7rVFKIgipIfJadJcUIQUjQanZXBhv1rWi/47hl3GixYAskTFogcOVbmTL2uIo7q3inQDaU1bQrB/tf4uX4VSCvnfOx7604p0HzSLRbGnJfG+pQ5Rsu6nm4SniPDzLk6D5QP3GCG15q7zdrIMswkEXAsdo3nR4NbbqeEM9FSWztI74RRDb8DoYcbdAb2O3EC9CT7rmvRCHrn3ZwF8uFv36DH3goKSkeTxWmdHvuUyIXXXfcdQK7q/CokAedSIhd9bKhz73HnFooxI7plYhqSLi+xK1tL3TGpJt7QtAgMBAAGjgYswgYgwEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFD2IAzoQUeH3raVegkIGSAdwmWMgMA0GCSqGSIb3DQEBCwUAA4ICAQC0XG83k2PJvEMXY5p47n/Unn8vbMzl8QxkIR4+mD06N46BWR5BkGms824h8XV8e9G3Ku5XhlJQQXnUGSBItrAKEAQZiI5O+5/w21wz6auPXVhesZs/ekatdYbJrTnSdk88nqs/rA9gAB7RBh/z9ydCmMGyYMvhN3e2ama/G40VicZ/wutB+bZY7Jqeh6Yp/ZgfyCCP4bR5UArqgHi4fGPNbh4KDNnxJ0SKfiCYfsiAZRcKl0rWun5HDjLD+IOZVrf4toHG7ssm/ljclOueaW+nHZA5dWgMnzAHhxCZxXoN3olZhIif1A8HSgmdi/yB0ReTHD2LRe7i0QQCJ7F+1w+hR0zeaHECBTZgid1Z4lp7/6yQHVQ6UNlJzDs+H1Sll/0H1Cd3qQiI+dLbfJkG+7D846eHM+RwVg71LGmwFwY5ETcA/Npt1sq0GtpTO3yYiZtkwLIy6psL/R7y54QQKD3BhoaPMzN8hgTKERrLfDcV+c5Hl7ngMotn/6VLaPJLljXVSMzHsJ2Iezua3vYB2FcyiEna7/UvceZlEViVEZzW4Smh4tEjHcx2gLy6shxEUjy8y49Mwfy94nFQpLLVwx1+AspL3xAQ9TGDt1arCnTj6bULuxmy+iE4lPjqjHe+Iv3WATHc6thpdQGJmCN06RATzJAPW8JaNVE5QDvGz3Jcvw==",
	})

	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid concurrent processing",
			config: Config{
				InputPath:    testGzipFile,
				OutputPath:   filepath.Join(tempDir, "concurrent_output.json"),
				OutputFormat: "json",
				WorkerCount:  2,
				BatchSize:    10,
				LogLevel:     "info",
			},
			wantErr: false,
		},
		{
			name: "single worker",
			config: Config{
				InputPath:    testGzipFile,
				OutputPath:   filepath.Join(tempDir, "single_worker.json"),
				OutputFormat: "json",
				WorkerCount:  1,
				BatchSize:    5,
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
				WorkerCount:  2,
			},
			wantErr: true,
			errMsg:  "invalid configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cp.ProcessFileWithWorkers(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ProcessFileWithWorkers() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ProcessFileWithWorkers() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ProcessFileWithWorkers() unexpected error = %v", err)
					return
				}

				// Verify output file was created
				if _, err := os.Stat(tt.config.OutputPath); os.IsNotExist(err) {
					t.Errorf("ProcessFileWithWorkers() did not create output file: %s", tt.config.OutputPath)
				}
			}
		})
	}
}

func TestConcurrentProcessor_ProcessBatchWithWorkers(t *testing.T) {
	cp := NewConcurrentProcessor(nil)
	tempDir := t.TempDir()

	// Create multiple test gzip files
	testFile1 := createTestGzipFileForConcurrent(t, tempDir, "batch1.gz", []string{
		"0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MDkxNDAzNDVaFw0xNzA4MjUxOTE3NTlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIyODc0MjUwNzczMDkwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCOhY+wZx0Ek4w1BUaRtFJjmc8ZfkeiwSlCYue6EDiTyTZrpvkTzoBac9qgakH4Eh47Vr5fwGJicH2mKJ06nmzXkKwhvShxKN/eC95sEoYmUtiWPrW3IaDugNfLyzFQn19L9d8n71QFaEK7cUBE2+xNvJ5OkHOvD7hiuhvk2IRSwB12q0vp2eDhbEyf574XHnqk6R+ncauIQlRvv8rJ8MXRhzDCQScZolUL24xbpAv2UqxU0dfL8ZT0Ux0ebCL7Y+boVF5CbYaIiW0/qnW64yc87AhBsOTgqx1Qcq6qUhRm0tD8k4MmdCUhfo3YF9B34HOX0HsQhCaiMUtsRFQ0OdOxAgMBAAGjgaAwgZ0wEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFM4DfrlPGK8DoEi5z/ruSOyzqt+iMBMGCisGAQQB1nkCBAMBAf8EAgUAMA0GCSqGSIb3DQEBCwUAA4ICAQBOilO5T+iijwaJu4wk9nypEAiPc1i+fsT+T8oVhgKSi7vaOQs4LqA2Ay+2YVqfn5fwxuLdDGlNBtJUVa9tsZWIlQn7z4r7gSvCgo1Uvyu+gGEFsZ9uTrsHf6x0UuPW+ExPKgcsOVhno/UDi9zWz56JS9HF/mYwMpIwlqFaJOBlN1eMMoVx8rt8LAivHeaJScMyaQurZ3qWdEN5mSl7EmPuf6YYjZ8heZcn861BQLVc0SOW/ahWll16z2yYCoB93q2eGYDMD0Aw2kX1XFPemPAR5zsxzQyzMyqDH3NPc+lC2z7JKftkbJ9QzTxO2lzSYdnTt1ZJEimXerJe3tuabUaWcxNHPPOwGYVlL1yi+miYF97lbxgphNpkPHcrVp3k/p08feGbDaIQl8bD82Xme3wScTKT5J4PLlNiEPuOH8vIweOTkwGQjyLwE+4FX9RQ9ftCvIs8HqD0QxrQlKcRhcTLpfl1sXeWnHMzJWlGnh7qGrD82RvuXOxh2egIQ6BMFKr6s9lau1kunxU4YoyAQtgzWPk0hDfOUbk/WCX3d0gPQoaldRBSsFR7mwtI//M71+lAHI0hSBzxkgdaDyhO2a2upY5f50rCpT3Ef/UOpiq5hEYbw80ukn0oLxY53gU6BDz0Hh29pNJzCdJp4bbVYbRscxqKf7t7WfPYaVnnUaHVzg==",
	})

	testFile2 := createTestGzipFileForConcurrent(t, tempDir, "batch2.gz", []string{
		"1 MIIE7zCCAtegAwIBAgIHBVZlioDhNTANBgkqhkiG9w0BAQsFADB/MQswCQYDVQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMRcwFQYDVQQKDA5Hb29nbGUgVUsgTHRkLjEhMB8GA1UECwwYQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MSMwIQYDVQQDDBpNZXJnZSBEZWxheSBJbnRlcm1lZGlhdGUgMTAeFw0xNzA4MTAxMjQzMThaFw0xNzA4MzAwOTQ2MjlaMGMxCzAJBgNVBAYTAkdCMQ8wDQYDVQQHDAZMb25kb24xKDAmBgNVBAoMH0dvb2dsZSBDZXJ0aWZpY2F0ZSBUcmFuc3BhcmVuY3kxGTAXBgNVBAUTEDE1MDIzNjg5OTg5NDA5ODEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC8IayGYkGmso9thX5tfE/cl1Fic82bbKTU8Vapo1A4m1YPgJnd7rVFKIgipIfJadJcUIQUjQanZXBhv1rWi/47hl3GixYAskTFogcOVbmTL2uIo7q3inQDaU1bQrB/tf4uX4VSCvnfOx7604p0HzSLRbGnJfG+pQ5Rsu6nm4SniPDzLk6D5QP3GCG15q7zdrIMswkEXAsdo3nR4NbbqeEM9FSWztI74RRDb8DoYcbdAb2O3EC9CT7rmvRCHrn3ZwF8uFv36DH3goKSkeTxWmdHvuUyIXXXfcdQK7q/CokAedSIhd9bKhz73HnFooxI7plYhqSLi+xK1tL3TGpJt7QtAgMBAAGjgYswgYgwEwYDVR0lBAwwCgYIKwYBBQUHAwEwIwYDVR0RBBwwGoIYZmxvd2Vycy10by10aGUtd29ybGQuY29tMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU6TwE4YAvwoQTLSZwnvL9Gs+q/sYwHQYDVR0OBBYEFD2IAzoQUeH3raVegkIGSAdwmWMgMA0GCSqGSIb3DQEBCwUAA4ICAQC0XG83k2PJvEMXY5p47n/Unn8vbMzl8QxkIR4+mD06N46BWR5BkGms824h8XV8e9G3Ku5XhlJQQXnUGSBItrAKEAQZiI5O+5/w21wz6auPXVhesZs/ekatdYbJrTnSdk88nqs/rA9gAB7RBh/z9ydCmMGyYMvhN3e2ama/G40VicZ/wutB+bZY7Jqeh6Yp/ZgfyCCP4bR5UArqgHi4fGPNbh4KDNnxJ0SKfiCYfsiAZRcKl0rWun5HDjLD+IOZVrf4toHG7ssm/ljclOueaW+nHZA5dWgMnzAHhxCZxXoN3olZhIif1A8HSgmdi/yB0ReTHD2LRe7i0QQCJ7F+1w+hR0zeaHECBTZgid1Z4lp7/6yQHVQ6UNlJzDs+H1Sll/0H1Cd3qQiI+dLbfJkG+7D846eHM+RwVg71LGmwFwY5ETcA/Npt1sq0GtpTO3yYiZtkwLIy6psL/R7y54QQKD3BhoaPMzN8hgTKERrLfDcV+c5Hl7ngMotn/6VLaPJLljXVSMzHsJ2Iezua3vYB2FcyiEna7/UvceZlEViVEZzW4Smh4tEjHcx2gLy6shxEUjy8y49Mwfy94nFQpLLVwx1+AspL3xAQ9TGDt1arCnTj6bULuxmy+iE4lPjqjHe+Iv3WATHc6thpdQGJmCN06RATzJAPW8JaNVE5QDvGz3Jcvw==",
	})

	config := Config{
		OutputPath:   filepath.Join(tempDir, "concurrent_batch_output.json"),
		OutputFormat: "json",
		WorkerCount:  2,
		BatchSize:    10,
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
			name:       "valid concurrent batch processing",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cp.ProcessBatchWithWorkers(tt.inputPaths, tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ProcessBatchWithWorkers() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ProcessBatchWithWorkers() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ProcessBatchWithWorkers() unexpected error = %v", err)
					return
				}

				// Verify output file was created
				if _, err := os.Stat(tt.config.OutputPath); os.IsNotExist(err) {
					t.Errorf("ProcessBatchWithWorkers() did not create output file: %s", tt.config.OutputPath)
				}
			}
		})
	}
}

func TestWorkerPool(t *testing.T) {
	cp := NewConcurrentProcessor(nil)
	
	// Test worker pool creation
	wp := NewWorkerPool(cp, 2, 10)
	if wp == nil {
		t.Fatal("NewWorkerPool() returned nil")
	}
	if wp.workerCount != 2 {
		t.Errorf("NewWorkerPool() workerCount = %d, want 2", wp.workerCount)
	}
	if wp.batchSize != 10 {
		t.Errorf("NewWorkerPool() batchSize = %d, want 10", wp.batchSize)
	}

	// Start the worker pool
	wp.Start()

	// Test submitting work (this is a basic test - in real usage, you'd process results)
	entry := &CertificateEntry{
		SequenceNumber:  1,
		CertificateData: "test_data",
	}

	// Submit should work initially
	err := wp.Submit(entry)
	if err != nil {
		t.Errorf("Submit() unexpected error = %v", err)
	}

	// Stop the worker pool
	wp.Stop()

	// Submit should fail after stopping (but we need to handle the panic)
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic when submitting to closed channel
				t.Logf("Expected panic when submitting to stopped worker pool: %v", r)
			}
		}()
		err = wp.Submit(entry)
		if err == nil {
			t.Error("Submit() expected error after stopping but got none")
		}
	}()
}

func TestBackpressureController(t *testing.T) {
	bc := NewBackpressureController(5, nil)
	if bc == nil {
		t.Fatal("NewBackpressureController() returned nil")
	}

	// Initially should be able to accept work
	if !bc.CanAccept() {
		t.Error("CanAccept() should return true initially")
	}

	// Add work up to capacity
	for i := 0; i < 5; i++ {
		if err := bc.AddWork(); err != nil {
			t.Errorf("AddWork() unexpected error at %d: %v", i, err)
		}
	}

	// Should not be able to accept more work
	if bc.CanAccept() {
		t.Error("CanAccept() should return false when at capacity")
	}

	// Adding more work should fail
	if err := bc.AddWork(); err == nil {
		t.Error("AddWork() expected error when at capacity but got none")
	}

	// Check queue size
	if size := bc.GetQueueSize(); size != 5 {
		t.Errorf("GetQueueSize() = %d, want 5", size)
	}

	// Check utilization
	if util := bc.GetUtilization(); util != 100.0 {
		t.Errorf("GetUtilization() = %.2f, want 100.00", util)
	}

	// Complete some work
	bc.CompleteWork()
	bc.CompleteWork()

	// Should be able to accept work again
	if !bc.CanAccept() {
		t.Error("CanAccept() should return true after completing work")
	}

	// Check updated queue size
	if size := bc.GetQueueSize(); size != 3 {
		t.Errorf("GetQueueSize() = %d, want 3", size)
	}

	// Check updated utilization
	if util := bc.GetUtilization(); util != 60.0 {
		t.Errorf("GetUtilization() = %.2f, want 60.00", util)
	}
}

// Helper function to create test gzip files for concurrent testing
func createTestGzipFileForConcurrent(t *testing.T, dir, filename string, lines []string) string {
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