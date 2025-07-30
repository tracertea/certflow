package ctprocessor

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestNewCertificateParser(t *testing.T) {
	cp := NewCertificateParser()
	if cp == nil {
		t.Fatal("NewCertificateParser() returned nil")
	}
}

func TestCertificateParser_DecodeBase64Certificate(t *testing.T) {
	cp := NewCertificateParser()

	// Create test certificate data
	testData := "Hello, World!"
	encodedData := base64.StdEncoding.EncodeToString([]byte(testData))

	tests := []struct {
		name     string
		certData string
		want     []byte
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "valid base64 data",
			certData: encodedData,
			want:     []byte(testData),
			wantErr:  false,
		},
		{
			name:     "empty certificate data",
			certData: "",
			wantErr:  true,
			errMsg:   "certificate data is empty",
		},
		{
			name:     "whitespace only certificate data",
			certData: "   \t  ",
			wantErr:  true,
			errMsg:   "certificate data is empty",
		},
		{
			name:     "invalid base64 data",
			certData: "invalid-base64-data!@#",
			wantErr:  true,
			errMsg:   "failed to decode base64 certificate data",
		},
		{
			name:     "base64 data with whitespace",
			certData: " " + encodedData[:10] + " \t " + encodedData[10:] + " \n ",
			want:     []byte(testData),
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cp.DecodeBase64Certificate(tt.certData)
			if tt.wantErr {
				if err == nil {
					t.Errorf("DecodeBase64Certificate() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("DecodeBase64Certificate() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("DecodeBase64Certificate() unexpected error = %v", err)
					return
				}
				if string(got) != string(tt.want) {
					t.Errorf("DecodeBase64Certificate() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestCertificateParser_ParseX509Certificate(t *testing.T) {
	cp := NewCertificateParser()

	// Create a test certificate
	testCert := createTestCertificate(t)
	validDER := testCert.Raw

	tests := []struct {
		name    string
		derData []byte
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid DER data",
			derData: validDER,
			wantErr: false,
		},
		{
			name:    "empty DER data",
			derData: []byte{},
			wantErr: true,
			errMsg:  "certificate DER data is empty",
		},
		{
			name:    "nil DER data",
			derData: nil,
			wantErr: true,
			errMsg:  "certificate DER data is empty",
		},
		{
			name:    "invalid DER data",
			derData: []byte{0x01, 0x02, 0x03, 0x04},
			wantErr: true,
			errMsg:  "failed to parse X.509 certificate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cp.ParseX509Certificate(tt.derData)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseX509Certificate() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ParseX509Certificate() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ParseX509Certificate() unexpected error = %v", err)
					return
				}
				if got == nil {
					t.Errorf("ParseX509Certificate() returned nil certificate")
				}
			}
		})
	}
}

func TestCertificateParser_ParseCertificateEntry(t *testing.T) {
	cp := NewCertificateParser()

	// Create test certificate and encode it
	testCert := createTestCertificate(t)
	encodedCert := base64.StdEncoding.EncodeToString(testCert.Raw)

	tests := []struct {
		name    string
		entry   *CertificateEntry
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid certificate entry",
			entry: &CertificateEntry{
				SequenceNumber:  123,
				CertificateData: encodedCert,
			},
			wantErr: false,
		},
		{
			name:    "nil certificate entry",
			entry:   nil,
			wantErr: true,
			errMsg:  "certificate entry is nil",
		},
		{
			name: "invalid certificate entry",
			entry: &CertificateEntry{
				SequenceNumber:  -1,
				CertificateData: encodedCert,
			},
			wantErr: true,
			errMsg:  "invalid certificate entry",
		},
		{
			name: "invalid base64 data",
			entry: &CertificateEntry{
				SequenceNumber:  123,
				CertificateData: "invalid-base64",
			},
			wantErr: true,
			errMsg:  "failed to decode certificate",
		},
		{
			name: "invalid certificate data",
			entry: &CertificateEntry{
				SequenceNumber:  123,
				CertificateData: base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03}),
			},
			wantErr: true,
			errMsg:  "failed to parse certificate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cp.ParseCertificateEntry(tt.entry)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseCertificateEntry() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ParseCertificateEntry() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ParseCertificateEntry() unexpected error = %v", err)
					return
				}
				if tt.entry.ParsedCert == nil {
					t.Errorf("ParseCertificateEntry() did not set ParsedCert")
				}
			}
		})
	}
}

func TestCertificateParser_ExtractDomainMappings(t *testing.T) {
	cp := NewCertificateParser()

	tests := []struct {
		name         string
		cert         *x509.Certificate
		wantCount    int
		wantErr      bool
		errMsg       string
		checkMapping func([]DomainMapping) bool
	}{
		{
			name:      "nil certificate",
			cert:      nil,
			wantErr:   true,
			errMsg:    "certificate is nil",
		},
		{
			name:      "certificate with CN and organization",
			cert:      createTestCertificateWithNames(t, "example.com", []string{"api.example.com", "www.example.com"}, "Example Corp"),
			wantCount: 3, // 1 CN + 2 SANs
			wantErr:   false,
			checkMapping: func(mappings []DomainMapping) bool {
				cnFound := false
				sanCount := 0
				for _, m := range mappings {
					if m.Source == "CN" && m.Name == "example.com" && m.Organization == "Example Corp" {
						cnFound = true
					}
					if m.Source == "SAN" && m.Organization == "Example Corp" {
						sanCount++
					}
				}
				return cnFound && sanCount == 2
			},
		},
		{
			name:      "certificate with no organization",
			cert:      createTestCertificateWithNames(t, "example.com", []string{"api.example.com"}, ""),
			wantCount: 0, // No mappings without organization
			wantErr:   false,
		},
		{
			name:      "certificate with only CN",
			cert:      createTestCertificateWithNames(t, "example.com", []string{}, "Example Corp"),
			wantCount: 1, // Only CN
			wantErr:   false,
			checkMapping: func(mappings []DomainMapping) bool {
				return len(mappings) == 1 && mappings[0].Source == "CN" && mappings[0].Name == "example.com"
			},
		},
		{
			name:      "certificate with only SANs",
			cert:      createTestCertificateWithNames(t, "", []string{"api.example.com", "www.example.com"}, "Example Corp"),
			wantCount: 2, // Only SANs
			wantErr:   false,
			checkMapping: func(mappings []DomainMapping) bool {
				sanCount := 0
				for _, m := range mappings {
					if m.Source == "SAN" {
						sanCount++
					}
				}
				return sanCount == 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cp.ExtractDomainMappings(tt.cert)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ExtractDomainMappings() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ExtractDomainMappings() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ExtractDomainMappings() unexpected error = %v", err)
					return
				}
				if len(got) != tt.wantCount {
					t.Errorf("ExtractDomainMappings() returned %d mappings, want %d", len(got), tt.wantCount)
				}
				if tt.checkMapping != nil && !tt.checkMapping(got) {
					t.Errorf("ExtractDomainMappings() mappings validation failed")
				}
			}
		})
	}
}

func TestCertificateParser_ProcessCertificateWithRecovery(t *testing.T) {
	cp := NewCertificateParser()

	// Create test certificate and encode it
	testCert := createTestCertificateWithNames(t, "example.com", []string{"api.example.com"}, "Example Corp")
	encodedCert := base64.StdEncoding.EncodeToString(testCert.Raw)

	entry := &CertificateEntry{
		SequenceNumber:  123,
		CertificateData: encodedCert,
	}

	mappings, err := cp.ProcessCertificateWithRecovery(entry)
	if err != nil {
		t.Fatalf("ProcessCertificateWithRecovery() unexpected error = %v", err)
	}

	if len(mappings) != 2 {
		t.Errorf("ProcessCertificateWithRecovery() returned %d mappings, want 2", len(mappings))
	}

	// Check that sequence numbers are set
	for _, mapping := range mappings {
		if mapping.SequenceNum != 123 {
			t.Errorf("ProcessCertificateWithRecovery() mapping sequence number = %d, want 123", mapping.SequenceNum)
		}
	}
}

func TestCertificateParser_ValidateCertificate(t *testing.T) {
	cp := NewCertificateParser()

	tests := []struct {
		name    string
		cert    *x509.Certificate
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil certificate",
			cert:    nil,
			wantErr: true,
			errMsg:  "certificate is nil",
		},
		{
			name:    "valid certificate",
			cert:    createTestCertificateWithNames(t, "example.com", []string{"api.example.com"}, "Example Corp"),
			wantErr: false,
		},
		{
			name:    "certificate with no names",
			cert:    createTestCertificateWithNames(t, "", []string{}, "Example Corp"),
			wantErr: true,
			errMsg:  "certificate has no valid names",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cp.ValidateCertificate(tt.cert)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateCertificate() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateCertificate() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateCertificate() unexpected error = %v", err)
				}
			}
		})
	}
}

// Helper functions for creating test certificates

func createTestCertificate(t *testing.T) *x509.Certificate {
	return createTestCertificateWithNames(t, "test.example.com", []string{"api.test.example.com"}, "Test Corp")
}

func createTestCertificateWithNames(t testing.TB, cn string, sans []string, org string) *x509.Certificate {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     sans,
	}

	if org != "" {
		template.Subject.Organization = []string{org}
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Parse the certificate
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("Failed to parse created certificate: %v", err)
	}

	return cert
}